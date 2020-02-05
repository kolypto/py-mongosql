import warnings
from itertools import chain, repeat
from copy import copy

from sqlalchemy import inspect, TypeDecorator
from sqlalchemy import Column
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.ext.associationproxy import AssociationProxy
from sqlalchemy.ext.hybrid import hybrid_property

from typing import Union, Set, Mapping, Iterable, Tuple, FrozenSet, List
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import ColumnProperty, RelationshipProperty
from sqlalchemy.orm.base import InspectionAttr
from sqlalchemy.orm.interfaces import MapperProperty
from sqlalchemy.orm.util import AliasedClass
from sqlalchemy.sql.elements import BinaryExpression
from sqlalchemy.sql.type_api import TypeEngine

from mongosql import SA_12, SA_13
try: from sqlalchemy.ext.associationproxy import ColumnAssociationProxyInstance  # SA 1.3.x
except ImportError: ColumnAssociationProxyInstance = None


class ModelPropertyBags:
    """ Model Property Bags is the class that lets you get information about the model's columns.

    This is the class that binds them all together: Columns, Relationships, PKs, etc.
    All the meta-information about a certain Model is stored here:

    - Columns
    - Relationships
    - Primary keys
    - Nullable columns
    - Properties and Hybrid Properties
    - Columns of related models
    - Writable properties

    Whenever it's too much to inspect several properties, use a `CombinedBag()` over them,
    which lets you get a column from a number of bags.
    """
    __bags_per_model_cache = {}

    @classmethod
    def for_model(cls, model: DeclarativeMeta) -> 'ModelPropertyBags':
        """ Get bags for a model.

        Please use this method over __init__(), because it initializes those bags only once
        """
        # The goal of this method is to only initialize a ModelPropertyBags only once per model.
        # Previously, we used to store them inside model attributes.

        try:
            # We want ever model class to have its own ModelPropertyBags,
            # and we want no one to inherit it.
            # We could use model.__dict__ for this, but classes in Python 3 use an immutable `mappingproxy` instead.
            # Thus, we have to keep our own cache of ModelPropertyBags.
            return cls.__bags_per_model_cache[model]
        except KeyError:
            cls.__bags_per_model_cache[model] = bags = cls(model)
            return bags

    @classmethod
    def for_alias(cls, aliased_model: AliasedClass) -> 'ModelPropertyBags':
        """ Get bags for an aliased class """
        model = inspect(aliased_model).class_
        return cls.for_model(model).aliased(aliased_model)

    @classmethod
    def for_model_or_alias(cls, target: Union[DeclarativeMeta, AliasedClass]) -> 'ModelPropertyBags':
        """ Get bags for a model, or aliased(model) """
        if inspect(target).is_aliased_class:
            return cls.for_alias(target)
        else:
            return cls.for_model(target)

    def __init__(self, model: DeclarativeMeta):
        """ Init bags

        :param model: Model
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        # We don't tolerate aliases here
        if inspect(model).is_aliased_class:
            raise TypeError('MongoPropertyBags does not tolerate aliased() models.'
                            'If you do really need to use one, do it this way: '
                            'ModelPropertyBags.for_alias(aliased_model)')

        # Get the inspector
        insp = inspect(model)

        # Initialize
        self.model = model
        self.model_name = model.__name__

        # Init bags: after every column type
        self.columns = self._init_columns(model, insp)
        self.properties = self._init_properties(model, insp)
        self.hybrid_properties = self._init_hybrid_properties(model, insp)
        self.association_proxies = self._init_association_proxies(model, insp)
        self.relations = self._init_relations(model, insp)
        self.related_columns = self._init_related_columns(model, insp)

        # Additional informational bags
        self.pk = self._init_primary_key(model, insp)
        self.nullable = self._init_nullable_columns(model, insp)

        # Writable entities
        self.writable_properties = self._init_writable_properties(model, insp)
        self.writable_hybrid_properties = self._init_writable_hybrid_properties(model, insp)

        self.writable = CombinedBag(
            # Everything that's writable in a model (excluding relations)
            col=self.columns,
            prop=self.writable_properties,
            hybrid=self.writable_hybrid_properties,
        )

    # region: Initialize bags

    # A bunch of initialization methods
    # This way, you can override the way a model is analyzed, and bags initialized

    def _init_columns(self, model, insp):
        """ Initialize: Column properties """
        return DotColumnsBag(_get_model_columns(model, insp))

    def _init_properties(self, model, insp):
        """ Initialize: Calculated properties: @property """
        return PropertiesBag(_get_model_properties(model, insp))

    def _init_hybrid_properties(self, model, insp):
        """ Initialize: Hybrid properties """
        return HybridPropertiesBag(_get_model_hybrid_properties(model, insp))

    def _init_association_proxies(self, model, insp):
        """ Initialize: association proxies """
        return AssociationProxiesBag(_get_model_association_proxies(model, insp))

    def _init_relations(self, model, insp):
        """ Initialize: Relationships and related columns """
        #: Relationship properties
        relationships_dict = _get_model_relationships(model, insp)
        return RelationshipsBag(relationships_dict)

    def _init_related_columns(self, model, insp):
        #: Related column properties
        relationships_dict = _get_model_relationships(model, insp)
        return DotRelatedColumnsBag(relationships_dict)

    def _init_primary_key(self, model, insp):
        """ Initialize: Primary key columns """
        #: Primary key columns
        return PrimaryKeyBag({c.name: self.columns[c.name]
                              for c in insp.primary_key})

    def _init_nullable_columns(self, model, insp):
        """ Initialize: Nullable columns """
        #: Nullable columns
        return ColumnsBag({name: c
                           for name, c in self.columns
                           if c.nullable})

    def _init_writable_properties(self, model, insp):
        """ Initialize: writable properties """
        return PropertiesBag({name: None
                              for name in self.properties.names
                              if _is_property_writable(getattr(model, name))})

    def _init_writable_hybrid_properties(self, model, insp):
        """ Initialize: writable Hybrid properties """
        return HybridPropertiesBag({name: prop
                                    for name, prop in self.hybrid_properties
                                    if _is_property_writable(prop)})

    # endregion

    def aliased(self, aliased_class: AliasedClass):
        # Return a wrapper that will lazily apply aliased() on every property when accessed
        # This makes sense because we don't know which of the bags are going to be actually used,
        # and aliased() has a bit of overhead: it involves copying the whole class.
        # Benchmarks have shown that it's about 3 times faster.
        return _MPB_LazyAliasedWrapper(self.__dict__, aliased_class)

    @property
    def all_names(self) -> Set[str]:
        """ Get the names of all properties defined for the model """
        return self.columns.names | \
               self.properties.names | \
               self.hybrid_properties.names | \
               self.association_proxies.names | \
               self.relations.names


class _PropertiesBagBase:
    """ Base class for Property bags:

    A container that keeps meta-information on SqlAlchemy stuff, like:
    - Columns
    - Primary keys
    - Relations
    - Related columns
    - Hybrid properties
    - Regular python properties

    There typically is a class that implements specific needs for every kind of property.

    Since there are so many different container types, there's one, CombinedBag(), that can
    handle them all, depending on the context.
    """

    def __init__(self) -> None:
        super().__init__()
        self._aliased_insp = None

    def __contains__(self, name: str) -> bool:
        """ Test if the property is in the bag
        :param name: Property name
        """
        raise NotImplementedError

    def __getitem__(self, name: str) -> MapperProperty:
        """ Get the property by name
        :param name: Property name
        """
        raise NotImplementedError

    def __copy__(self) -> '_PropertiesBagBase':
        """ Copy behavior is used to make an AliasedBag """
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def aliased(self, aliased_class) -> '_PropertiesBagBase':
        """ Get a version of this bag for using with an aliased class """
        new = copy(self)
        new._aliased_insp = inspect(aliased_class)
        return new

    @property
    def names(self) -> FrozenSet[str]:
        """ Get the set of names """
        raise NotImplementedError

    def __iter__(self) -> Mapping[str, MapperProperty]:
        """ Get all items """
        raise NotImplementedError

    def get_invalid_names(self, names: Iterable[str]) -> Set[str]:
        """ Get the names of invalid items

        Use this for validation.
        """
        return set(names) - self.names


class PropertiesBag(_PropertiesBagBase):
    """ Contains simple model properties (@property) """

    def __init__(self, properties: Mapping[str, None]):
        super(PropertiesBag, self).__init__()
        self._property_names = frozenset(properties.keys())

    @property
    def names(self) -> FrozenSet[str]:
        """ Get the set of property names """
        return self._property_names

    def __contains__(self, prop_name: str) -> bool:
        return prop_name in self._property_names

    def __getitem__(self, prop_name: str) -> None:
        if prop_name in self._property_names:
            return None
        raise KeyError(prop_name)

    def __iter__(self) -> Iterable[Tuple[str, None]]:
        return iter(zip(self._property_names, repeat(None)))


class _ColumnLikeAttrsBagBase(_PropertiesBagBase):
    """ Bag for column-like attributes (like association proxies) """

    def __init__(self, column_like_attrs: Mapping[str, InspectionAttr]):
        """ Init Association Proxies """
        super(_ColumnLikeAttrsBagBase, self).__init__()
        self._columns = column_like_attrs
        self._column_names = frozenset(self._columns.keys())

    @property
    def names(self) -> FrozenSet[str]:
        return self._column_names

    def __iter__(self) -> Iterable[Tuple[str, InspectionAttr]]:
        return iter(self._columns.items())

    def __contains__(self, name: str) -> bool:
        return name in self._column_names

    def __getitem__(self, column_name: str) -> InspectionAttr:
        return self._columns[column_name]

    def is_column_array(self, name: str) -> bool:
        """ Is the column an ARRAY column """
        raise NotImplementedError

    def is_column_json(self, name: str) -> bool:
        """ Is the column a JSON column """
        raise NotImplementedError


class ColumnsBag(_ColumnLikeAttrsBagBase):
    """ Columns bag

    Contains meta-information about columns:
    - which of them are ARRAY, or JSON types
    - list of their names
    - list of all columns
    - getting a column by name: bag[column_name]
    """

    def __init__(self, columns: Mapping[str, ColumnProperty]):
        """ Init columns

        :param columns: Model columns
        """
        super(ColumnsBag, self).__init__(columns)

        # More info about columns based on their type
        self._array_column_names = frozenset(name
                                             for name, col in self._columns.items()
                                             if _is_column_array(col))
        self._json_column_names =  frozenset(name
                                             for name, col in self._columns.items()
                                             if _is_column_json(col))

    def aliased(self, aliased_class: AliasedClass):
        return DictOfAliasedColumns.aliased_attrs(
            aliased_class,
            super(ColumnsBag, self).aliased(aliased_class),
            '_columns'
        )

    def is_column_array(self, name: str) -> bool:
        column_name = get_plain_column_name(name)
        return column_name in self._array_column_names

    def is_column_json(self, name: str) -> bool:
        column_name = get_plain_column_name(name)
        return column_name in self._json_column_names


class HybridPropertiesBag(ColumnsBag):
    """ Contains hybrid properties of a model """

    class _Hack_Lazy_Dict:
        """ A Lazy dict that only loads its keys upon request """
        __slots__ = ('_l', '_ks')

        def __init__(self, keys, lambda_value):
            self._ks = keys
            self._l = lambda_value

        def __getitem__(self, key):
            return self._l(key)

        def items(self):
            return ((k, self._l(k))
                    for k in self._ks)

    def aliased(self, aliased_class: AliasedClass) -> 'HybridPropertiesBag':
        new = super(HybridPropertiesBag, self).aliased(aliased_class)
        # For some reason, hybrid properties do not get a proper alias with adapt_to_entity()
        # We have to get them the usual way: from the entity
        # TODO: This method is a hack and is not supposed to be here at all. I've got to find out
        #   why hybrid methods got through this wrapper dictionary are not getting a proper alias!
        #   It seems that adapt_to_entity() is somehow insufficient. Perhaps, it is only manifest
        #   when an alias has an explicitly set name with aliased(name=...) ?
        #   When the bug is solved, this method should be removed completely.

        # new._columns = {col_name: getattr(aliased_class, col_name)
        #                 for col_name in self._column_names}
        # Don't use a real dict; use a lazy wrapper
        new._columns = self._Hack_Lazy_Dict(
            self._column_names,
            lambda col_name: getattr(aliased_class, col_name))

        return new


class PrimaryKeyBag(ColumnsBag):
    """ Primary Key Bag

    Like ColumnBag, but with a fancy name :)
    """


class DotColumnsBag(ColumnsBag):
    """ Columns bag with additional capabilities:

        - For JSON fields: field.prop.prop -- dot-notation access to sub-properties
    """

    def __contains__(self, name: str) -> bool:
        column_name, path = _dot_notation(name)
        return super(DotColumnsBag, self).__contains__(column_name)

    def __getitem__(self, name: str) -> Union[ColumnProperty, BinaryExpression]:
        column_name, path = _dot_notation(name)
        col = super(DotColumnsBag, self).__getitem__(column_name)
        # JSON path
        if path:
            if self.is_column_json(column_name):
                col = col[path].astext
            else:
                raise KeyError(name)
        return col

    def get_column_name(self, name: str) -> str:
        """ Get a column name, not a JSON path """
        return get_plain_column_name(name)

    def get_column(self, name: str) -> ColumnProperty:
        """ Get a column, not a JSON path """
        return self[get_plain_column_name(name)]

    def get_invalid_names(self, names: Iterable[str]) -> Set[str]:
        # First, validate easy names
        invalid = super(DotColumnsBag, self).get_invalid_names(names)  #type: set
        # Next, among those invalid ones, give those with dot-notation a second change: they
        # might be JSON columns' fields!
        invalid -= {name
                    for name in invalid
                    if self.is_column_json(name)
                    }
        return invalid


class AssociationProxiesBag(_ColumnLikeAttrsBagBase):
    """ Bag for Association Proxies """

    # Implement those two methods so that it looks like a column

    def is_column_array(self, name: str) -> bool:
        # Well, even though this column is clearly an array, it does not behave like one when thought of in terms of
        # Postgres operators, because the underlying comparison is done to a scalar column.
        # Example: AssociationProxy to User.name will use the `name` column for comparisons, which is scalar!
        return False

    def is_column_json(self, name: str) -> bool:
        return False

    def get_relationship(self, assoc_proxy: ColumnAssociationProxyInstance):
        """ Get the underlying relationship """
        # Get the relationship
        relationship = assoc_proxy.local_attr

        # When aliased, the relationship has to be adapted
        if self._aliased_insp:
            relationship = relationship.adapt_to_entity(self._aliased_insp)

        # Done
        return relationship


class RelationshipsBag(_PropertiesBagBase):
    """ Relationships bag

    Keeps track of relationships of a model.
    """

    def __init__(self, relationships: Mapping[str, RelationshipProperty]):
        """ Init relationships
        :param relationships: Model relationships
        """
        super(RelationshipsBag, self).__init__()
        self._relations = relationships
        self._rel_names = frozenset(self._relations.keys())
        self._array_rel_names = frozenset(name
                                          for name, rel in self._relations.items()
                                          if _is_relationship_array(rel))

    def aliased(self, aliased_class: AliasedClass) -> 'RelationshipsBag':
        return DictOfAliasedColumns.aliased_attrs(
            aliased_class,
            super(RelationshipsBag, self).aliased(aliased_class),
            '_relations'
        )

    def is_relationship_array(self, name: str) -> bool:
        """ Is the relationship an array relationship? """
        return name in self._array_rel_names

    @property
    def names(self) -> FrozenSet[str]:
        """ Get the set of relation names """
        return self._rel_names

    def __iter__(self) -> Iterable[Tuple[str, RelationshipProperty]]:
        """ Get relationships """
        return iter(self._relations.items())

    def __contains__(self, name: str) -> bool:
        return name in self._relations

    def __getitem__(self, name: str) -> RelationshipProperty:
        return self._relations[name]

    def get_target_model(self, name: str) -> DeclarativeMeta:
        """ Get target model of a relationship """
        return self[name].property.mapper.class_


class DotRelatedColumnsBag(ColumnsBag):
    """ Relationships bag that supports dot-notation for referencing columns of a related model """

    def __init__(self, relationships: Mapping[str, ColumnProperty]):
        self._rel_bag = RelationshipsBag(relationships)

        #: Dot-notation mapped to columns: 'rel.col' => Column
        related_columns = {}
        #: Dot-notation mapped to target models: 'rel.col' => Model, and 'rel' => Model
        rel_col_2_model = {}

        # Collect columns from every relation
        for rel_name, relation in self._rel_bag:
            # Get the model
            model = relation.property.mapper.class_
            rel_col_2_model[rel_name] = model

            # Get the columns
            ins = inspect(model)
            cols = _get_model_columns(model, ins)  # TODO: support more attr types? hybrid? association proxy?

            # Remember all of them, using dot-notation
            for col_name, col in cols.items():
                key = '{}.{}'.format(rel_name, col_name)
                related_columns[key] = col
                rel_col_2_model[key] = model

        # Now, when we have enough information, call super().__init__
        # It will initialize:
        # `._columns`,
        # `._column_names`,
        # `._array_column_names`,
        # `._json_column_names`
        # Keep in mind that all of them are RELATED COLUMNS
        super(DotRelatedColumnsBag, self).__init__(related_columns)

        #: A mapping of related column names to target models
        #self._column_name_to_related_model = rel_col_2_model  # unused

    def aliased(self, aliased_class: AliasedClass) -> 'DotRelatedColumnsBag':
        new = DictOfAliasedColumns.aliased_attrs(
            aliased_class,
            super(DotRelatedColumnsBag, self).aliased(aliased_class),
            '_columns', #'_column_name_to_related_model',
        )
        new._rel_bag = new._rel_bag.aliased(aliased_class)
        return new

    def is_column_array(self, name: str) -> bool:
        # not dot-notation filter like in the parent class: check as is!
        return name in self._array_column_names

    def is_column_json(self, name: str) -> bool:
        # not dot-notation filter like in the parent class: check as is!
        return name in self._json_column_names

    def get_relationship_name(self, col_name: str) -> str:
        return _dot_notation(col_name)[0]

    def get_related_column_name(self, col_name: str) -> str:
        return _dot_notation(col_name)[1]

    def get_relationship(self, col_name: str) -> RelationshipProperty:
        return self._rel_bag[self.get_relationship_name(col_name)]

    def is_relationship_array(self, col_name: str) -> bool:
        """ Is this relationship an array?

            This method accepts both relationship names and its column names.
            That is, both 'users' and 'users.id' will actually tell you about a relationship itself.
        """
        rel_name = get_plain_column_name(col_name)
        return self._rel_bag.is_relationship_array(rel_name)


class FakeBag(_PropertiesBagBase):
    """ A bag that supports dot-notation and contains fake column names that do not actually exist.

        This is used to support legacy columns. They are assumed to support dot-notation.
    """

    def __init__(self, fake_columns: Mapping[str, None]):
        super(FakeBag, self).__init__()
        self._fake_columns = fake_columns
        self._fake_column_names = frozenset(self._fake_columns.keys())

    def aliased(self, aliased_class: AliasedClass):
        return self  # same thing

    @property
    def names(self) -> FrozenSet[str]:
        return self._fake_column_names

    def __iter__(self) -> Iterable[Tuple[str, None]]:
        return iter(self._fake_columns.items())

    def __contains__(self, name: str) -> bool:
        return get_plain_column_name(name) in self._fake_columns

    def __getitem__(self, name: str) -> Union[ColumnProperty, BinaryExpression]:
        return self._fake_columns[get_plain_column_name(name)]

    def get_invalid_names(self, names: Iterable[str]) -> Set[str]:
        return {name
                for name in names
                if get_plain_column_name(name) not in self._fake_column_names
                }


class CombinedBag(_PropertiesBagBase):
    """ A bag that combines elements from multiple bags.

    This one is used when something can handle both columns and relationships, or properties and
    columns. Because this depends on what you're doing, this generalized implementation is used.

    In order to initialize it, you give them the bags you need as a dict:

        cbag = CombinedBag(
            col=bags.columns,
            rel=bags.related_columns,
        )

    Now, when you get an item, you get the aliased name that you have used:

        bag_name, bag, col = cbag['id']
        bag_name  #-> 'col'
        bag  #-> bags.columns
        col  #-> User.id

    This way, you can always tell which bag has the column come from, and handle it appropriately.
    """

    def __init__(self, **bags):
        super(CombinedBag, self).__init__()
        self._bags = bags

        # Combined names from all bags
        self._names = frozenset(chain(*(bag.names for bag in bags.values())))

        # Combined lookup by name from all bags
        self._bag_name_lookup_by_column_name = {
            column_name: bag_name
            for bag_name, bag  in self._bags.items()
            for column_name, column in bag
        }

        # List of JSON columns
        json_column_names = []
        for bag in self._bags.values():
            # We'll access a protected property, so got to make sure we've got the right class
            if isinstance(bag, ColumnsBag):
                # Get the list of JSON columns from a ColumnsBag
                json_column_names.extend(bag._json_column_names)
            elif isinstance(bag, FakeBag):
                # Get the list of fake columns from a Fake bag
                json_column_names.extend(bag.names)
        self._json_column_names = frozenset(json_column_names)

    def aliased(self, aliased_class: AliasedClass) -> 'CombinedBag':
        new = super(CombinedBag, self).aliased(aliased_class)
        # aliased() on every bag
        new._bags = {name: bag.aliased(aliased_class)
                     for name, bag in self._bags.items()}
        return new

    def bag(self, name) -> _PropertiesBagBase:
        """ Get a specific bag by name """
        return self._bags[name]

    def __contains__(self, name: str) -> bool:
        # Simple
        if name in self._names:
            return True
        # It might be a JSON column. Try it
        if get_plain_column_name(name) in self._json_column_names:
            return True
        # Nope. Nothing worked
        return False

    def __getitem__(self, name: str) -> Tuple[str, _PropertiesBagBase, MapperProperty]:
        # Get the column name: remove the '.'-notation only if the column is a json column
        plain_name = get_plain_column_name(name)
        plain_name = plain_name if plain_name in self._json_column_names else name
        # Locate the bag by quick lookup
        bag_name = self._bag_name_lookup_by_column_name[plain_name]
        # Get the column
        bag = self._bags[bag_name]
        # Done
        return (bag_name, bag, bag[name])

    def get_invalid_names(self, names: Iterable[str]) -> Set[str]:
        # This method is copy-paste from ColumnsBag
        # First, validate easy names
        invalid = super(CombinedBag, self).get_invalid_names(names)  # type: set
        # Next, among those invalid ones, give those with dot-notation a second change: they
        # might be JSON columns' fields!
        invalid -= {name
                    for name in invalid
                    if get_plain_column_name(name) in self._json_column_names
                    }
        return invalid

    def get(self, name: str) -> MapperProperty:
        """ Get a property """
        return self[name][2]

    @property
    def names(self) -> FrozenSet[str]:
        return self._names

    def __iter__(self) -> Iterable[Tuple[str, _PropertiesBagBase, str, MapperProperty]]:
        return (
            (bag_name, bag, column_name, column)
            for bag_name, bag in self._bags.items()
            for column_name, column in bag
        )


def _get_model_columns(model, ins):
    """ Get a dict of model columns """
    return {name: getattr(model, name)
            for name, c in ins.column_attrs.items()
            # ignore Labels and other stuff that .items() will always yield
            if isinstance(c.expression, Column)
            }


def _get_model_association_proxies(model, ins):
    """ Get a dict of model association_proxy attributes """
    # Ignore AssociationProxy attrs for SA 1.2.x
    if SA_12:
        warnings.warn('MongoSQL only supports AssociationProxy columns with SqlAlchemy 1.3.x')
        return {}

    return {name: getattr(model, name)
            for name, c in ins.all_orm_descriptors.items()
            if not name.startswith('_')
            and isinstance(c, AssociationProxy)}


def _get_model_hybrid_properties(model, ins):
    """ Get a dict of model hybrid properties """
    return {name: getattr(model, name)
            for name, c in ins.all_orm_descriptors.items()
            if not name.startswith('_')
            and isinstance(c, hybrid_property)}


def _get_model_properties(model, ins):
    """ Get a dict of model properties (calculated properties) """
    return {name: None  # we don't need the property itself
            for name in dir(model)
            if not name.startswith('_')
            and isinstance(getattr(model, name), property)}


def _get_model_relationships(model, ins):
    """ Get a dict of model relationships """
    return {name: getattr(model, name)
            for name, c in ins.relationships.items()}


def _get_column_type(col: MapperProperty) -> TypeEngine:
    """ Get column's SQL type """
    if isinstance(col.type, TypeDecorator):
        # Type decorators wrap other types, so we have to handle them carefully
        return col.type.impl
    else:
        return col.type


def _is_column_array(col: MapperProperty) -> bool:
    """ Is the column a PostgreSql ARRAY column? """
    return isinstance(_get_column_type(col), pg.ARRAY)


def _is_column_json(col: MapperProperty) -> bool:
    """ Is the column a PostgreSql JSON column? """
    return isinstance(_get_column_type(col), (pg.JSON, pg.JSONB))


def _is_relationship_array(rel: RelationshipProperty) -> bool:
    """ Is the relationship an array relationship? """
    return rel.property.uselist


def _is_property_writable(prop: property) -> bool:
    """ Check if a property is writable """
    return prop.fset is not None


def _dot_notation(name: str) -> Tuple[str, List[str]]:
    """ Split a property name that's using dot-notation.

    This is used to navigate the internals of JSON types:

        "json_column.property.property"
    """
    path = name.split('.')
    return path[0], path[1:]


def get_plain_column_name(name: str) -> str:
    """ Get a plain column name, dropping any dot-notation that may follow """
    return name.split('.')[0]


class DictOfAliasedColumns:
    """ A dict of columns that makes proper aliases upon access

        All our bags contain columns of a real model.
        However, in queries, we often need aliases, and need to get them transparently.

        To achieve that, we implement a dict that is capable of producing
        columns of an aliased model on demand.

        Upon access, adapt_to_entity() is called.
    """
    __slots__ = ('_d', '_a',)

    @classmethod
    def aliased_attrs(cls, aliased_class: AliasedClass, obj: object, *attr_names: str):
        """ Wrap a whole list of dictionaries into aliased wrappers """
        # Prepare AliasedInsp: this is what adapt_to_entity() wants
        aliased_inspector = inspect(aliased_class)
        assert aliased_inspector.is_aliased_class, '`aliased_class` must be an alias!'

        # Convert every attribute
        for attr_name in attr_names:
            setattr(obj, attr_name,
                    # Wrap it with self
                    cls(getattr(obj, attr_name),
                        aliased_inspector)
                    )

        # Done
        return obj

    def __init__(self, columns_dict, aliased_insp):
        """ Make a dict of columns, ready to alias them as needed """
        self._d = columns_dict
        self._a = aliased_insp

    def _adapt_to_entity(self, attr):
        """ Helper to adapt properties to aliases """
        return attr.adapt_to_entity(self._a)

    # adapters

    def __contains__(self, key):
        return key in self._d

    def __getitem__(self, key):
        return self._adapt_to_entity(self._d[key])

    def values(self):
        return (self._adapt_to_entity(c)
                for c in self._d.values())

    def items(self):
        return ((k, self._adapt_to_entity(c))
                for k, c in self._d.items())


class _MPB_LazyAliasedWrapper:
    """ A ModelPropertyBags wrapper that will lazily apply aliased() on every attribute upon access """
    def __init__(self, mpb_dict: dict, aliased_class: AliasedClass):
        self.__aliased_class = aliased_class

        # Remember those attributes that were not aliased() yet
        self.__unaliased = {}

        # Tell attributes apart:
        # set the bags aside for later aliased()ing,
        # but put all other attributes onto ourselves
        for k, v in mpb_dict.items():
            if isinstance(v, _PropertiesBagBase):
                self.__unaliased[k] = mpb_dict[k]
            else:
                setattr(self, k, v)  # onto ourselves

    def __getattr__(self, attr: str):
        # Initialize a new attribute that's aliased()
        setattr(self,
                attr,
                self.__unaliased.pop(attr).aliased(self.__aliased_class)
                )

        # return it
        return getattr(self, attr)

