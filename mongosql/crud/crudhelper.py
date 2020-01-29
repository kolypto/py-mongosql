"""
MongoSql is designed to help with data selection for the APIs.
To ease the pain of implementing CRUD for all of your models,
MongoSQL comes with a CRUD helper that exposes MongoSQL capabilities for querying to the API user.
Together with [RestfulView](https://github.com/kolypto/py-flask-jsontools#restfulview)
from [flask-jsontools](https://github.com/kolypto/py-flask-jsontools),
CRUD controllers are extremely easy to build.
"""

from sqlalchemy.orm import Query
from sqlalchemy.orm.attributes import flag_modified

from mongosql import exc
from mongosql import MongoQuery, ModelPropertyBags
from mongosql.util import Reusable

from typing import Union, Mapping, Iterable, Set, Callable, MutableMapping
from sqlalchemy.ext.declarative import DeclarativeMeta


class CrudHelper:
    """ Crud helper: an object that helps implement CRUD operations for an API endpoint:

        * Create: construct SqlAlchemy instances from the submitted entity dict
        * Read: use MongoQuery for querying
        * Update: update SqlAlchemy instances from the submitted entity using a dict
        * Delete: use MongoQuery for deletion

        Source: [mongosql/crud/crudhelper.py](mongosql/crud/crudhelper.py)

        This object is supposed to be initialized only once;
        don't do it for every query, keep it at the class level!

        Most likely, you'll want to keep it at the class level of your view:

        ```python
        from .models import User
        from mongosql import CrudHelper

        class UserView:
            crudhelper = CrudHelper(
                # The model to work with
                User,
                # Settings for MongoQuery
                **MongoQuerySettingsDict(
                    allowed_relations=('user_profile',),
                )
            )
            # ...
        ```

        Note that during "create" and "update" operations, this class lets you write values
        to column attributes, and also to @property that are writable (have a setter).
        If this behavior (with writable properties) is undesirable,
        set `writable_properties=False`

        The following methods are available:
    """

    # The class to use for getting structural data from a model
    _MODEL_PROPERTY_BAGS_CLS = ModelPropertyBags
    # The class to use for MongoQuery
    _MONGOQUERY_CLS = MongoQuery

    def __init__(self, model: DeclarativeMeta, writable_properties=True, **handler_settings):
        """ Init CRUD helper

        :param model: The model to work with
        :param handler_settings: Settings for the MongoQuery used to make queries
        """
        self.model = model
        self.handler_settings = handler_settings
        self.bags = self._MODEL_PROPERTY_BAGS_CLS.for_model(model)
        self.reusable_mongoquery = Reusable(self._MONGOQUERY_CLS(self.model, handler_settings))  # type: MongoQuery

        # Settings
        self.writable_properties = writable_properties

        # We also need `legacy_fields`
        # we're going to ignore them in the input
        self.legacy_fields = self.reusable_mongoquery.handler_project.legacy_fields

    def query_model(self, query_obj: Union[Mapping, None] = None, from_query: Union[Query, None] = None) -> MongoQuery:
        """ Make a MongoQuery using the provided Query Object

            Note that you have to provide the MongoQuery yourself.
            This is because it has to be properly configured with handler_settings.

            :param query_obj: The Query Object to use
            :param from_query: An optional Query to initialize MongoQuery with
            :raises exc.InvalidColumnError: Invalid column name specified in the Query Object by the user
            :raises exc.InvalidRelationError: Invalid relationship name specified in the Query Object by the user
            :raises exc.InvalidQueryError: There is an error in the Query Object that the user has made
            :raises exc.DisabledError: A feature is disabled; likely, due to a configuration issue. See handler_settings.
        """
        # Validate
        if not isinstance(query_obj, (Mapping, NoneType)):
            raise exc.InvalidQueryError('Query Object must be either an object, or null')

        # Query
        return self._query_model(query_obj or {}, from_query)  # ensure dict

    def _query_model(self, query_obj: Mapping, from_query: Union[Query, None] = None) -> MongoQuery:
        """ Make a MongoQuery """
        return self.reusable_mongoquery.from_query(from_query).query(**query_obj)

    def _validate_columns(self, column_names: Iterable[str], where: str) -> Set[str]:
        """ Validate column names

            :raises exc.InvalidColumnError: Invalid column name
        """
        unk_cols = self.bags.columns.get_invalid_names(column_names)
        if unk_cols:
            raise exc.InvalidColumnError(self.bags.model_name, unk_cols.pop(), where)
        return set(column_names)

    def _validate_attributes(self, column_names: Iterable[str], where: str) -> Set[str]:
        """ Validate attribute names (any, inc. properties)

            :raises exc.InvalidColumnError: Invalid column name
        """
        column_names = set(column_names)
        unk_cols = column_names - self.bags.all_names
        if unk_cols:
            raise exc.InvalidColumnError(self.bags.model_name, unk_cols.pop(), where)
        return column_names

    def _validate_writable_attributes(self, attr_names: Iterable[str], where: str) -> Set[str]:
        """ Validate attribute names (columns, properties, hybrid properties) that are writable

            This list does not include attributes like relationships and read-only properties

            :raises exc.InvalidColumnError: Column name was not writable
            :rtype: set[set]
        """
        attr_names = set(attr_names)
        unk_cols = attr_names - self.bags.writable.names
        if unk_cols:
            raise exc.InvalidColumnError(self.bags.model_name, unk_cols.pop(), where)
        return attr_names

    def validate_incoming_entity_dict_fields(self, entity_dict: dict, action: str) -> dict:
        """ Validate the incoming JSON data """
        # Validate
        if not isinstance(entity_dict, Mapping):
            raise exc.InvalidQueryError(f'Model "{action}": the value has to be an object, '
                                        f'not {type(entity_dict)}')

        # Remove certain fields from the entity dict
        if action == 'create':
            self._remove_entity_dict_fields(entity_dict, self._fields_to_remove_on_create)
        elif action == 'update':
            self._remove_entity_dict_fields(entity_dict, self._fields_to_remove_on_update)
        else:
            raise ValueError(action)

        # Check fields
        if self.writable_properties:
            # let both columns and @properties
            self._validate_writable_attributes(entity_dict.keys(), action)
        else:
            # let only columns
            self._validate_columns(entity_dict.keys(), action)

        # Done
        return entity_dict

    @property
    def _fields_to_remove_on_create(self):
        """ The list of fields to remove when creating an instance from an entity dict """
        return self.legacy_fields

    @property
    def _fields_to_remove_on_update(self):
        """ The list of fields to remove when updating an instance from an entity dict """
        return self.legacy_fields

    def _remove_entity_dict_fields(self, entity_dict: MutableMapping, rm_fields: Set[str]):
        """ Remove certain fields from the incoming entity dict """
        for k in set(entity_dict.keys()) & rm_fields:
            entity_dict.pop(k)

    def create_model(self, entity_dict: Mapping) -> object:
        """ Create an instance from entity dict.

            This method lets you set the value of columns and writable properties,
            but not relations. Use @saves_relations to handle additional fields.

            :param entity_dict: Entity dict
            :return: Created instance
            :raises InvalidQueryError: validation errors
            :raises InvalidColumnError: invalid column
        """
        # Validate and prepare it
        entity_dict = self.validate_incoming_entity_dict_fields(entity_dict, 'create')

        # Create
        return self._create_model(entity_dict)

    def _create_model(self, entity_dict: Mapping) -> object:
        """ Create an instance from a dict

            This method does not validate `entity_dict`
        """
        return self.model(**entity_dict)

    def update_model(self, entity_dict: Mapping, instance: object) -> object:
        """ Update an instance from an entity dict by merging the fields

            - Attributes are copied over
            - JSON dicts are shallowly merged

            Note that because properties are *copied over*,
            this operation does not replace the entity; it merely updates the entity.

            In other words, this method does a *partial update*:
            only updates the fields that were provided by the client, leaving all the rest intact.

            :param entity_dict: Entity dict
            :param instance: The instance to update
            :return: New instance, updated
            :raises InvalidQueryError: validation errors
            :raises InvalidColumnError: invalid column
        """
        # Validate and prepare it
        entity_dict = self.validate_incoming_entity_dict_fields(entity_dict, 'update')

        # Update
        return self._update_model(entity_dict, instance)

    def _update_model(self, entity_dict: Mapping, instance: object) -> object:
        """ Update an instance from an entity dict

            This method does not validate `entity_dict`
        """
        # Update
        for name, val in entity_dict.items():
            if isinstance(val, Mapping) and self.bags.columns.is_column_json(name):
                # JSON column with a dict: do a shallow merge
                getattr(instance, name).update(val)
                # Tell SqlAlchemy that a mutable collection was updated
                flag_modified(instance, name)
            else:
                # Other columns: just assign
                setattr(instance, name, val)

        # Finish
        return instance


class StrictCrudHelper(CrudHelper):
    """ A Strict Crud Helper imposes defaults and limitations on the API user:

        Source: [mongosql/crud/crudhelper.py](mongosql/crud/crudhelper.py)

        - Read-only fields can not be set: not with create, nor with update
        - Constant fields can be set initially, but never be updated
        - Defaults for Query Object provide the default values for every query, unless overridden

        The following behavior is implemented:

        * By default, all fields are writable
        * If ro_fields is provided, these fields become read-only, all other fields are writable
        * If rw_fields is provided, ony these fields are writable, all other fields are read-only
        * If const_fields, it is seen as a further limitation on rw_fields: those fields would be writable,
            but only once.

        Attributes:
            writable_properties (bool): Enable saving values from incoming JSON into @property attrs?
            ro_fields (set[str]): The list of read-only field names
            rw_fields (set[str]): The list of writable field names
            const_fields (set[str]): The list of constant field names
            query_defaults (dict): Default values for every field of the Query Object
    """

    def __init__(self, model: DeclarativeMeta,
                 writable_properties: bool = True,
                 ro_fields: Union[Iterable[str], Callable, None] = None,
                 rw_fields: Union[Iterable[str], Callable, None] = None,
                 const_fields: Union[Iterable[str], Callable, None] = None,
                 query_defaults: Union[Iterable[str], Callable, None] = None,
                 **handler_settings):
        """ Initializes a strict CRUD helper

            Note: use a `**StrictCrudHelperSettingsDict()` to help you with the argument names and their docs!

            Args:
                model: The model to work with
                writable_properties: enable writing to @property attributes?
                ro_fields: List of read-only property names, or a callable which gives the list
                rw_fields: List of writable property names, or a callable which gives the list
                const_fields: List of property names that are constant once set, or a callable which gives the list
                query_defaults: Defaults for every Query Object: Query Object will be merged into it.
                handler_settings: Settings for the `MongoQuery` used to make queries
                writable_properties:

            Example:

                ```python
                from .models import User
                from mongosql import StrictCrudHelper, StrictCrudHelperSettingsDict

                class UserView:
                    crudhelper = StrictCrudHelper(
                        # The model to work with
                        User,
                        # Settings for MongoQuery and StrictCrudHelper
                        **StrictCrudHelperSettingsDict(
                            # Can never be set of modified
                            ro_fields=('id',),
                            # Can only be set once
                            const_fields=('login',),
                            # Relations that can be `join`ed
                            allowed_relations=('user_profile',),
                        )
                    )
                    # ...
                ```
        """
        super().__init__(model, writable_properties=writable_properties, **handler_settings)

        # ro, rw, const fields
        ro, rw, cn = self._init_ro_rw_cn_fields(ro_fields, rw_fields, const_fields)
        self.ro_fields = ro
        self.rw_fields = rw
        self.const_fields = cn

        # Defaults for the Query Object
        self.query_defaults = query_defaults or {}  # type: dict

        # Validate the Default Query Object
        MongoQuery(self.model).query(**self.query_defaults)

    def _init_ro_rw_cn_fields(self, ro_fields, rw_fields, cn_fields):
        """ Initialize ro_fields and rw_fields and const_fields

            :rtype: (set[str], set[str], set[str])
        """
        # Usage
        ro_provided = ro_fields is not None  # provided, even if empty
        rw_provided = rw_fields is not None
        if ro_provided and rw_provided:
            raise ValueError('Use either `ro_fields` or `rw_fields`, but not both')

        # Read-only and Read-Write fields
        ro_fields = set(call_if_callable(ro_fields)) if ro_fields is not None else set()
        rw_fields = set(call_if_callable(rw_fields)) if rw_fields is not None else set()
        cn_fields = set(call_if_callable(cn_fields)) if cn_fields is not None else set()

        # Validate
        self._validate_attributes(ro_fields, 'ro_fields')
        self._validate_writable_attributes(rw_fields, 'rw_fields')
        self._validate_writable_attributes(cn_fields, 'const_fields')

        # ro_fields
        if rw_provided:
            ro_fields = set(self.bags.all_names - rw_fields - cn_fields)

        # rw_fields
        rw_fields = self.bags.writable.names - ro_fields - cn_fields

        # Done
        return frozenset(ro_fields), frozenset(rw_fields), frozenset(cn_fields)

    @property
    def _fields_to_remove_on_create(self):
        """ The list of fields to remove when creating an instance from an entity dict """
        return super()._fields_to_remove_on_create | self.ro_fields

    @property
    def _fields_to_remove_on_update(self):
        """ The list of fields to remove when updating an instance from an entity dict """
        return super()._fields_to_remove_on_update | self.ro_fields | self.const_fields

    def _query_model(self, query_obj: Mapping, from_query: Union[Query, None] = None) -> MongoQuery:
        # Default Query Object
        if self.query_defaults:
            query_obj = {**self.query_defaults, **(query_obj or {})}

        # Super
        return super()._query_model(query_obj, from_query=from_query)


NoneType = type(None)


def call_if_callable(v):
    """ Preprocess a value: return it ; but call it, if it's a lambda (for late binding) """
    return v() if callable(v) else v
