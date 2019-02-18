from __future__ import absolute_import
from itertools import chain, repeat

from sqlalchemy import inspect
from sqlalchemy import Column
from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.ext.hybrid import hybrid_property


class ModelPropertyBags(object):
    """ Model property bags

    This is the class that binds them all together: Columns, Relationships, PKs, etc.
    All the meta-information about a certain Model is stored here:

    - Columns
    - Relationships
    - Primary keys
    - Nullable columns
    - and whatnot

    Whenever it's too much to inspect several properties, use CombinedBag() over them.
    """

    @classmethod
    def for_model(cls, model):
        """ Get bags for a model.

        Please use this method over __init__(), because it initializes those bags only once

        :param model: Model
        :type model: mongosql.MongoSqlBase|sqlalchemy.ext.declarative.DeclarativeMeta
        :rtype: ModelPropertyBags
        """
        try:
            return model.__mongosql_bags
        except AttributeError:
            model.__mongosql_bags = cls(model)
            return model.__mongosql_bags

    @classmethod
    def for_model_or_alias(cls, target):
        # TODO: there should be no difference! This method has to be removed
        if inspect(target).is_aliased_class:
            return cls(target)
        else:
            return cls.for_model(target)

    def __init__(self, model):
        """ Init bags

        :param model: Model
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        # Get the correct inspector
        # When you use aliases (e.g. join to the same table multiple times), a different
        # inspector should be used
        # TODO: this code should not be here at all ; but MongoModel is used in JOINs in order to
        #  analyze aliased models, so we have to respect that for a while.
        ins = inspect(model)
        pure_model = model
        if ins.is_aliased_class:
            ins = ins.mapper
            #model = ins.entity
            pure_model = ins.entity

        # Initialize
        self.model = model
        self.model_name = model.__name__

        #: Column properties
        self.columns = DotColumnsBag(_get_model_columns(model, ins))

        #: Calculated properties: @property
        self.properties = PropertiesBag(_get_model_properties(pure_model, ins))

        #: Hybrid properties
        self.hybrid_properties = HybridPropertiesBag(_get_model_hybrid_properties(model, ins))

        #: Relationship properties
        relations = {name: getattr(model, name)
                     for name, c in ins.relationships.items()}
        self.relations = RelationshipsBag(relations)

        #: Primary key properties
        self.pk = PrimaryKeyBag({c.name: self.columns[c.name]
                                 for c in ins.primary_key})

        #: Nullable properties
        self.nullable = ColumnsBag({name: c
                                    for name, c in self.columns
                                    if c.nullable})

        #: Relationship column properties
        self.related_columns = DotRelatedColumnsBag(relations)


class PropertiesBagBase(object):
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

    def __contains__(self, name):
        """ Test if the property is in the bag
        :param name: Property name
        :type name: str
        :rtype: bool
        """
        raise NotImplementedError

    def __getitem__(self, name):
        """ Get the property by name
        :param name: Property name
        :type name: str
        :rtype: sqlalchemy.orm.interfaces.MapperProperty
        """
        raise NotImplementedError

    @property
    def names(self):
        """ Get the set of names
        :rtype: set[str]
        """
        raise NotImplementedError

    def __iter__(self):
        """ Get all items

        :rtype: dict
        """
        raise NotImplementedError

    def get_invalid_names(self, names):
        """ Get the names of invalid items

        Use this for validation.
        """
        return set(names) - self.names


class PropertiesBag(PropertiesBagBase):
    """ Contains simple model properties (@property) """

    def __init__(self, properties):
        self._property_names = set(properties.keys())

    @property
    def names(self):
        """ Get the set of column names
        :rtype: set[str]
        """
        return self._property_names

    def __contains__(self, prop_name):
        return prop_name in self._property_names

    def __getitem__(self, prop_name):
        if prop_name in self._property_names:
            return None
        raise KeyError(prop_name)

    def __iter__(self):
        return iter(zip(self._property_names, repeat(None)))


class ColumnsBag(PropertiesBagBase):
    """ Columns bag

    Contains meta-information about columns:
    - which of them are ARRAY, or JSON types
    - list of their names
    - list of all columns
    - getting a column by name: bag[column_name]
    """

    def __init__(self, columns):
        """ Init columns

        :param columns: Model columns
        :type columns: dict[sqlalchemy.orm.properties.ColumnProperty]
        """
        self._columns = columns
        self._column_names = set(self._columns.keys())
        self._array_columns = {name: col for name, col in self._columns.items() if _is_column_array(col)}
        self._json_columns =  {name: col for name, col in self._columns.items() if _is_column_json(col)}

    def is_column_array(self, name):
        """ Is the column an ARRAY column
        :type name: str
        :rtype: bool
        """
        column_name = _dot_notation(name)[0]
        return column_name in self._array_columns

    def is_column_json(self, name):
        """ Is the column a JSON column
        :type name: str
        :rtype: bool
        """
        column_name = _dot_notation(name)[0]
        return column_name in self._json_columns

    @property
    def names(self):
        """ Get the set of column names
        :rtype: set[str]
        """
        return self._column_names

    def __iter__(self):
        """ Get columns
        :rtype: dict[sqlalchemy.orm.properties.ColumnProperty]
        """
        return iter(self._columns.items())

    def __contains__(self, column_name):
        return column_name in self._columns

    def __getitem__(self, column_name):
        return self._columns[column_name]


class HybridPropertiesBag(ColumnsBag):
    """ Contains hybrid properties of a model """


class PrimaryKeyBag(ColumnsBag):
    """ Primary Key Bag

    Like ColumnBag, but with a fancy name :)
    """


class DotColumnsBag(ColumnsBag):
    """ Columns bag with additional capabilities:

        - For JSON fields: field.prop.prop -- dot-notation access to sub-properties
    """

    def __contains__(self, name):
        column_name, path = _dot_notation(name)
        return super(DotColumnsBag, self).__contains__(column_name)

    def __getitem__(self, name):
        column_name, path = _dot_notation(name)
        col = super(DotColumnsBag, self).__getitem__(column_name)
        # JSON path
        if path:
            if self.is_column_json(column_name):
                col = col[path].astext
            else:
                raise KeyError(name)
        return col

    def get_column(self, name):
        """ Get a column, not a JSON path """
        return self[_dot_notation(name)[0]]

    def get_invalid_names(self, names):
        # First, validate easy names
        invalid = super(DotColumnsBag, self).get_invalid_names(names)  #type: set
        # Next, among those invalid ones, give those with dot-notation a second change: they
        # might be JSON columns' fields!
        invalid -= {name
                    for name in invalid
                    if self.is_column_json(name)
                    }
        return invalid


class RelationshipsBag(PropertiesBagBase):
    """ Relationships bag

    Keeps track of relationships of a model.
    """

    def __init__(self, relationships):
        """ Init relationships
        :param relationships: Model relationships
        :type relationships: dict[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        self._rels = relationships
        self._rel_names = set(self._rels.keys())
        self._array_rel_names = set((name
                                     for name, rel in self._rels.items()
                                     if _is_relationship_array(rel)))

    def is_relationship_array(self, name):
        """ Is the relationship an array relationship?

            :type name: str
            :rtype: bool
        """
        return name in self._array_rel_names

    @property
    def names(self):
        """ Get the set of relation names

        :rtype: set[str]
        """
        return self._rel_names

    def __iter__(self):
        """ Get relationships

        :rtype: dict[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        return iter(self._rels.items())

    def __contains__(self, name):
        return name in self._rels

    def __getitem__(self, name):
        return self._rels[name]

    def get_target_model(self, name):
        """ Get target model of a relationship """
        return self[name].property.mapper.class_


class DotRelatedColumnsBag(ColumnsBag):
    """ Relationships bag that supports dot-notation for referencing columns of a related model """

    def __init__(self, relationships):
        self._rel_bag = RelationshipsBag(relationships)

        #: Dot-notation mapped to columns: 'rel.col' => Column
        self._rel_cols = {}
        #: Dot-notation mapped to models: 'rel.col' => Model
        self._rel_col_models = {}

        # Collect columns from every relation
        for rel_name, relation in self._rel_bag:
            # Get the model
            # There are two different ways, depending on the kind of relationship
            if relation.property.uselist:
                model = relation.property.mapper.class_
            else:
                model = relation.property.mapper.class_
            self._rel_col_models[rel_name] = model

            # Get the columns
            ins = inspect(model)
            cols = _get_model_columns(model, ins)

            # Remember all of them, using dot-notation
            for col_name, col in cols.items():
                key = '{}.{}'.format(rel_name, col_name)
                self._rel_cols[key] = col
                self._rel_col_models[key] = model

        #: The set of all possible relationship columns
        self._rel_col_names = set(self._rel_cols.keys())

        #: Names of array columns
        self._rel_array_col_names = {name: col
                                     for name, col in self._rel_cols.items()
                                     if _is_column_array(col)}
        self._rel_json_col_names = {name: col
                                    for name, col in self._rel_cols.items() if
                                    _is_column_json(col)}


    @property
    def names(self):
        """ Get the set of all related column names

        :rtype: set[str]
        """
        return self._rel_col_names

    def __iter__(self):
        """ Get related columns

        :rtype: dict[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        return iter(self._rel_cols.items())

    def __contains__(self, name):
        return name in self._rel_cols

    def __getitem__(self, name):
        return self._rel_cols[name]

    def get_invalid_names(self, names):
        return set(names) - self._rel_col_names

    def is_column_array(self, name):
        """ Is this related column an array? """
        return name in self._rel_array_col_names

    def is_column_json(self, name):
        return name in self._rel_json_col_names

    def get_relationship_name(self, col_name):
        return _dot_notation(col_name)[0]

    def get_relationship(self, col_name):
        return self._rel_bag[self.get_relationship_name(col_name)]

    def is_relationship_array(self, col_name):
        """ Is this relationship an array?

            This method accepts both relationship names and its column names.
            That is, both 'users' and 'users.id' will actually tell you about a relationship itself.
        """
        rel_name = _dot_notation(col_name)[0]
        return self._rel_bag.is_relationship_array(rel_name)


class CombinedBag(PropertiesBagBase):
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
        self._bags = bags
        # Combined names from all bags
        self._names = set(chain(*(bag.names for bag in bags.values())))

        # List of JSON columns
        self._json_column_names = set()
        for bag in self._bags.values():
            # We'll access a protected property, so got to make sure we've got the right class
            if isinstance(bag, ColumnsBag) and not isinstance(bag, DotRelatedColumnsBag):
                self._json_column_names.update(bag._json_columns.keys())

    def __contains__(self, name):
        # Simple
        if name in self._names:
            return True
        # It might be a JSON column. Try it
        if _dot_notation(name)[0] in self._json_column_names:
            return True
        # Nope. Nothing worked
        return False

    def __getitem__(self, name):
        # Try every bag in order
        for bag_name, bag in self._bags.items():
            try:
                return (bag_name, bag, bag[name])
            except KeyError:
                continue
        raise KeyError(name)

    def get_invalid_names(self, names):
        # This method is copy-paste from ColumnsBag
        # First, validate easy names
        invalid = super(CombinedBag, self).get_invalid_names(names)  # type: set
        # Next, among those invalid ones, give those with dot-notation a second change: they
        # might be JSON columns' fields!
        invalid -= {name
                    for name in invalid
                    if _dot_notation(name)[0] in self._json_column_names
                    }
        return invalid

    def get(self, name):
        """ Get a property """
        return self[name][2]

    @property
    def names(self):
        return self._names

    def __iter__(self):
        return chain(*self._bags.values())


def _get_model_columns(model, ins):
    """ Get a dict of model columns """
    return {name: getattr(model, name)
            for name, c in ins.column_attrs.items()
            # ignore Labels and other stuff that .items() will always yield
            if isinstance(c.expression, Column)
            }

def _get_model_hybrid_properties(model, ins):
    """ Get a dict of model hybrid properties and regular properties """
    return {name: getattr(model, name)
            for name, c in ins.all_orm_descriptors.items()
            if isinstance(c, hybrid_property)}

def _get_model_properties(model, ins):
    """ Get a dict of model properties (calculated properies) """
    return {name: prop
            for name, prop in model.__dict__.items()
            if isinstance(prop, property)}

def _is_column_array(col):
    """ Is the column a PostgreSql ARRAY column?

    :type col: sqlalchemy.sql.schema.Column
    :rtype: bool
    """
    return isinstance(col.type, pg.ARRAY)


def _is_column_json(col):
    """ Is the column a PostgreSql JSON column?

    :type col: sqlalchemy.sql.schema.Column
    :rtype: bool
    """
    return isinstance(col.type, (pg.JSON, pg.JSONB))


def _is_relationship_array(rel):
    """ Is the relationship an array relationship?

    :type rel: sqlalchemy.orm.relationships.RelationshipProperty
    :rtype: bool
    """
    return rel.property.uselist


def _dot_notation(name):
    """ Split a property name that's using dot-notation.

    This is used to navigate the internals of JSON types:

        "json_colum.property.property"

    :type name: str
    :rtype: str, list[str]
    """
    path = name.split('.')
    return path[0], path[1:]
