from __future__ import absolute_import

from sqlalchemy.orm.attributes import flag_modified

from mongosql import exc
from mongosql import MongoQuery, ModelPropertyBags
from mongosql.util import Reusable


class CrudHelper(object):
    """ Crud helper: an object that helps implement CRUD

        Create: construct sqlalchemy instances from dict
        Read: use MongoQuery for querying
        Update: update sqlalchemy instances using a dict
        Delete: use MongoQuery for deletion

        This object is supposed to be ininitialized only once;
        don't do it for every query, keep it at the class level!
    """

    def __init__(self, model, **handler_settings):
        """ Init CRUD helper

        :param model: The model to work with
        :type model: DeclarativeMeta
        :param handler_settings: Settings for the MongoQuery used to make queries
        """
        self.model = model
        self.bags = ModelPropertyBags.for_model(model)
        self.reusable_mongoquery = Reusable(MongoQuery(self.model, handler_settings))

    def query_model(self, query_obj=None, from_query=None):
        """ Make a MongoQuery using the provided Query Object

            Note that you have to provide the MongoQuery yourself.
            This is because it has to be properly configured with handler_settings.

            :param query_obj: The Query Object to use
            :type query_obj: dict | None
            :param from_query: An optional Query to initialize MongoQuery with
            :type from_query: Query | None
            :rtype: MongoQuery
            :raises exc.InvalidColumnError: Invalid column name specified in the Query Object by the user
            :raises exc.InvalidRelationError: Invalid relationship name specified in the Query Object by the user
            :raises exc.InvalidQueryError: There is an error in the Query Object that the user has made
            :raises exc.DisabledError: A feature is disabled; likely, due to a configuration issue. See handler_settings.
        """
        # Validate
        if not isinstance(query_obj, (dict, NoneType)):
            raise exc.InvalidQueryError('Query Object must be either an object, or null')

        # Query
        return self._query_model(query_obj or {}, from_query)  # ensure dict

    def _query_model(self, query_obj, from_query=None):
        """ Make a MongoQuery """
        return self.reusable_mongoquery.from_query(from_query).query(**query_obj)

    def _validate_columns(self, column_names, where):
        """ Validate column names

            :raises exc.InvalidColumnError: Invalid column name
        """
        unk_cols = self.bags.columns.get_invalid_names(column_names)
        if unk_cols:
            raise exc.InvalidColumnError(self.bags.model_name, unk_cols.pop(), where)

    def create_model(self, entity_dict):
        """ Create an instance from entity dict.

            This only allows to assign column properties and not relations.

            :param entity_dict: Entity dict
            :type entity_dict: dict
            :return: Created instance
            :rtype: DeclarativeMeta
            :raises InvalidQueryError: validation errors
            :raises InvalidColumnError: invalid column
        """
        # Validate
        if not isinstance(entity_dict, dict):
            raise exc.InvalidQueryError('Create model: the value has to be an object, not {}'
                                        .format(type(entity_dict)))

        # Check columns
        self._validate_columns(entity_dict.keys(), 'create')

        # Create
        return self._create_model(entity_dict)

    def _create_model(self, entity_dict):
        """ Create an instance from a dict

            This method does not validate `entity_dict`
        """
        return self.model(**entity_dict)

    def update_model(self, entity_dict, instance):
        """ Update an instance from an entity dict by merging the fields

            - Properties are copied over
            - JSON dicts are shallowly merged

            Note that because properties are *copied over*,
            this operation does not replace the entity; it merely updates the entity.

            :param entity_dict: Entity dict
            :type entity_dict: dict
            :param instance: The instance to update
            :type instance: DeclarativeMeta
            :return: New instance, updated
            :rtype: DeclarativeMeta
            :raises InvalidQueryError: validation errors
            :raises InvalidColumnError: invalid column
        """
        # Validate
        if not isinstance(entity_dict, dict):
            raise exc.InvalidQueryError('Update model: the value has to be an object, not {}'
                                        .format(type(entity_dict)))

        # Check columns
        self._validate_columns(entity_dict.keys(), 'update')

        # Update
        return self._update_model(entity_dict, instance)

    def _update_model(self, entity_dict, instance):
        """ Update an instance from an entity dict

            This method does not validate `entity_dict`
        """
        # Update
        for name, val in entity_dict.items():
            if isinstance(val, dict) and self.bags.columns.is_column_json(name):
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
    """ Crud helper with limitations

        - Read-only fields can not be set: not with create, nor with update
        - Defaults for Query Object provide the default values for every query, unless overridden
    """

    def __init__(self, model, ro_fields=None, rw_fields=None, query_defaults=None, **handler_settings):
        """ Init a strict CRUD helper

            Note: use a **StrictCrudHelperSettingsDict() to help you with the argument names and their docs!

            :param model: The model to work with
            :param ro_fields: List of read-only property names, or a callable which gives the list
            :type ro_fields: Iterable[str] | Callable | None
            :param rw_fields: List of writable property names, or a callable which gives the list
            :type rw_fields: Iterable[str] | Callable | None
            :param query_defaults: Defaults for every Query Object: Query Object will be merged into it.
            :type query_defaults: dict | None
            :param handler_settings: Settings for the MongoQuery used to make queries
        """
        super(StrictCrudHelper, self).__init__(model, **handler_settings)

        # ro_fields
        self.ro_fields = self._init_ro_rw_fields(ro_fields, rw_fields)  # type: set[str]

        # Defaults for the Query Object
        self.query_defaults = query_defaults or {}  # type: dict

        # Validate the Default Query Object
        MongoQuery(self.model).query(**self.query_defaults)

    def _init_ro_rw_fields(self, ro_fields, rw_fields):
        """ Initialize ro_fields and rw_fields

            :rtype: set[str]
        """
        # Read-only fields
        assert not (ro_fields and rw_fields), 'Use either ro_fields or rw_fields, but not both'
        ro_fields = set(call_if_callable(ro_fields) or ())
        rw_fields = set(call_if_callable(rw_fields) or ())

        # Validate
        self._validate_columns(ro_fields, 'ro_fields')
        self._validate_columns(rw_fields, 'rw_fields')

        # Rw fields
        if rw_fields:
            ro_fields = set(self.bags.columns.names - rw_fields)

        # Done
        return ro_fields

    def _create_model(self, entity_dict):
        # Remove ro fields
        for k in set(entity_dict.keys()) & self.ro_fields:
            entity_dict.pop(k)

        # Super
        return super(StrictCrudHelper, self)._create_model(entity_dict)

    def _update_model(self, entity_dict, instance):
        # Remove ro fields
        for k in set(entity_dict.keys()) & self.ro_fields:
            entity_dict.pop(k)

        # Super
        return super(StrictCrudHelper, self)._update_model(entity_dict, instance)

    def _query_model(self, query_obj, from_query=None):
        # Default Query Object
        if self.query_defaults:
            query_obj = shallow_merge_dicts(self.query_defaults, query_obj or {})

        # Super
        return super(StrictCrudHelper, self)._query_model(query_obj, from_query=from_query)


NoneType = type(None)


def call_if_callable(v):
    """ Preprocess a value: return it ; but call it, if it's a lambda (for late binding) """
    return v() if callable(v) else v


def shallow_merge_dicts(d1, d2):
    """ Merge two dicts, d2 into d1, shallowly """
    d = {}
    d.update(d1)
    d.update(d2)
    return d
