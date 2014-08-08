from sqlalchemy.orm import ColumnProperty, RelationshipProperty
from sqlalchemy.util import KeyedTuple

from . import MongoModel, MongoQuery


class CrudHelper(object):
    """ Crud helper functions """

    def __init__(self, model):
        """ Init CRUD helper

        :param model: The model to work with
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        self.model = model
        self.mongomodel = MongoModel.get_for(self.model)

    def mquery(self, query, query_obj=None):
        """ Construct a MongoQuery for the model.

        If `query` is provided, it's used for initial filtering

        :param query: Query to start with
        :type query: sqlalchemy.orm.Query
        :param query_obj: Apply initial filtering with the Query Object
        :type query_obj: dict|None
        :rtype: mongosql.MongoQuery
        :raises AssertionError: unknown operations specified in query_obj
        """
        assert query_obj is None or isinstance(query_obj, dict), 'Query Object should be a dict or None'

        mq = MongoQuery(self.mongomodel, query)
        if query_obj:
            mq = mq.query(**query_obj)
        return mq

    def check_columns(self, names):
        """ Test if all column names are known

        :param names: Column names
        :type names: Iterable
        :return: Set of unknown names
        :rtype: set
        """
        model_colnames = self.mongomodel.model_bag.columns.names
        names = set(names)
        return names - model_colnames

    def nullify_empty_fields(self, entity):
        """ Walk through the entity dict and handle nullable fields:

        - If a field has a value of '', set it to None

        :param entity: Entity
        :type entity:
        :return: Altered entity
        :rtype: dict
        """
        for k in self.mongomodel.model_bag.nullable.keys():
            if k in entity and entity[k] == '':
                entity[k] = None
        return entity

    def create_model(self, entity):
        """ Create a model from entity dict.

        This only allows to assign column properties and not relations.

        :param entity: Entity dict
        :type entity: dict
        :return: Created model
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises AssertionError: validation errors
        """
        assert isinstance(entity, dict), 'Create model: entity should be a dict'

        # Check columns
        unk_cols = self.check_columns(entity.keys())
        assert not unk_cols, 'Create model: unknown fields: {}'.format(unk_cols)

        # Create
        return self.model(**entity)

    def update_model(self, model, entity):
        """ Update a model from entity dict by merging the fields

        - Properties are copied over
        - JSON dicts are shallowly merged

        :param model: Initial model
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        :param entity: Entity dict
        :type entity: dict
        :return: Updated model
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises AssertionError: validation errors
        """
        assert isinstance(entity, dict), 'Update model: entity should be a dict'

        # Check columns
        unk_cols = self.check_columns(entity.keys())
        assert not unk_cols, 'Update model: unknown fields: {}'.format(unk_cols)

        # Update
        for name, val in entity.items():
            if isinstance(val, dict) and self.mongomodel.model_bag.columns.is_column_json(name):
                # JSON column
                # NOTE: the field is very capricious to change management!
                p = dict(getattr(model, name)) or {}  # Defaults to empty dict
                for k, v in val.items():
                    p[k] = v  # Can't use update(): psycopg then raises 'TypeError: can't escape unicode to binary' o_O
                setattr(model, name, p)  # so SQLalchemy knows the field is updated
            else:
                # Other columns
                setattr(model, name, val)

        # Finish
        return model
    
    def replace_model(self, entity, prev_model=None):
        """ Replace a model with an entity dict

        :param entity: New entity dict
        :type entity: dict
        :param prev_model: Previous version of the same model, if any
        :type prev_model: sqlalchemy.ext.declarative.DeclarativeMeta|None
        :return: The new model
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        return self.create_model(entity)


class StrictCrudHelper(CrudHelper):
    """ Crud helper with limitations

        - Read-only fields can not be set
        - Only allowed relationships can be loaded
        - Default Query Object is used
        - Limits the maximum number of items that can be retrieved when listing
    """

    def __init__(self, model, ro_fields=(), allow_relations=(), query_defaults=None, maxitems=None):
        """ Init Strict CRUD helper

        :param model: The model to work with
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        :param ro_fields: List of read-only properties
        :type ro_fields: Iterable[str|sqlalchemy.orm.properties.ColumnProperty]
        :param allow_relations: List of relations allowed to join to.
            Specify relation names or relationship properties.
            To allow joining to second-level relations, use dot-notation.
        :type allow_relations: Iterable[str|sqlalchemy.orm.relationships.RelationshipProperty]
        :param query_defaults: Default Query Object used when nothing is specified
        :type query_defaults: dict|None
        :param maxitems: Hard limit on the number of entities that can be loaded (max value for QueryObject['limit'])
        :type maxitems: int|None
        """
        super(StrictCrudHelper, self).__init__(model)

        self._ro_fields = set(c.key if isinstance(c, ColumnProperty) else c for c in ro_fields)
        self._allowed_relations = set(c.key if isinstance(c, RelationshipProperty) else c for c in allow_relations)
        self._query_defaults = query_defaults or {}
        self._maxitems = maxitems or None

    @classmethod
    def _check_relations(cls, allowed_relations, qo, _prefix=''):
        """ Test Query Object joins against `allowed_relations`, supporting dot-notation

        :param allowed_relations: Set of allowed relations
        :type allowed_relations: set
        :param qo: Query Object
        :type qo: dict | None
        :returns: Banned relationships
        :rtype: set
        """
        if not isinstance(qo, dict) or 'join' not in qo:
            return set()
        joinspec = qo['join']

        relnames = {_prefix + name for name in joinspec}
        disallowed_relations = relnames - allowed_relations

        # Deeper
        if isinstance(joinspec, dict):
            for relname, qo in joinspec.items():
                disallowed_relations |= cls._check_relations(allowed_relations, qo, _prefix=relname + '.')

        # Finish
        return disallowed_relations

    def mquery(self, query, query_obj=None):
        assert query_obj is None or isinstance(query_obj, dict), 'Query Object should be a dict or None'

        # Query defaults
        if self._query_defaults:
            query_obj = dict(self._query_defaults.items() + (query_obj.items() if query_obj else []))

        # Max items
        if self._maxitems:
            query_obj = query_obj or {}
            if not (query_obj.get('count', 0) or query_obj.get('aggregate', 0)):  # no limits in count() and aggregate() modes
                query_obj['limit'] = min(self._maxitems, query_obj.get('limit', self._maxitems))

        # Allowed relations
        disallowed_relations = self._check_relations(self._allowed_relations, query_obj)
        assert not disallowed_relations, 'Joining to these relations is not allowed: {}'.format(disallowed_relations)

        # Finish
        return super(StrictCrudHelper, self).mquery(query, query_obj)

    def create_model(self, entity):
        assert isinstance(entity, dict), 'Create model: entity should be a dict'

        # Remove ro fields
        for k in set(entity.keys()) & self._ro_fields:
            entity.pop(k)
        return super(StrictCrudHelper, self).create_model(entity)

    def update_model(self, model, entity):
        assert isinstance(entity, dict), 'Update model: entity should be a dict'

        # Remove ro fields
        for k in set(entity.keys()) & self._ro_fields:
            entity.pop(k)
        return super(StrictCrudHelper, self).update_model(model, entity)

    def replace_model(self, entity, prev_model=None):
        model = super(StrictCrudHelper, self).replace_model(entity, prev_model)

        # Copy ro fields over
        if prev_model:
            for name in self._ro_fields:
                setattr(model, name, getattr(prev_model, name))
        return model


class CrudViewMixin(object):
    """ Base class for CRUD implementations """

    #: Set the CRUD helper object
    crudhelper = None

    @classmethod
    def _getCrudHelper(cls):
        """ Get the CRUD helper assigned for this class

        :rtype: mongosql.CrudHelper
        """
        assert isinstance(cls.crudhelper, CrudHelper), '{}: {} should be set to an instance of {}'.format(cls, 'crudhelper', CrudHelper)
        return cls.crudhelper

    def _query(self):
        """ Get a Query object to be used for queries

        :rtype: sqlalchemy.orm.Query
        """
        raise NotImplementedError('query() method not defined on {}'.format(type(self)))

    def _mquery(self, query_obj=None, *filter, **filter_by):
        """ Get a MongoQuery with initial filtering applied

        :param query_obj: Query Object
        :type query_obj: dict|None
        :param filter: Additional filter() criteria
        :param filter_by: Additional filter_by() criteria
        :rtype: mongosql.MongoQuery
        """
        return self._getCrudHelper().mquery(
            self._query().filter(*filter).filter_by(**filter_by),
            query_obj
        )

    def _get_one(self, query_obj, *filter, **filter_by):
        """ Utility method that fetches a single entity.

        You will probably want to override it with custom error handling

        :param query_obj: Query Object
        :param filter: Additional filter() criteria
        :param filter_by: Additional filter_by() criteria
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        return self._mquery(query_obj, *filter, **filter_by).end().one()

    def _save_hook(self, method, new, prev=None):
        """ Hook into create(), replace(), update() methods.

        This allows to make some changes to the model before it's actually saved.

        :param method: Method name: 'create', 'replace', 'update'
        :type method: str
        :param new: New version of the model
        :type new: sqlalchemy.ext.declarative.DeclarativeMeta
        :param prev: Previously persisted version of the model (only available for 'replace' and 'update')
        :type prev: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        pass

    def _method_list(self, query_obj=None, *filter, **filter_by):
        """ Fetch the list of entitites

        :param query_obj: Query Object
        :param filter: Additional filter() criteria
        :param filter_by: Additional filter_by() criteria
        :rtype: list
        :raises AssertionError: validation errors
        """
        res = self._mquery(query_obj, *filter, **filter_by).end().all()

        # Count?
        if query_obj and query_obj.get('count', 0):
            return res[0][0]  # Scalar count query

        # Convert KeyedTuples to dicts (when aggregating)
        if query_obj and 'aggregate' in query_obj:
            return [dict(zip(row.keys(), row)) for row in res]

        return res

    def _method_create(self, entity):
        """ Create a new entity

        :param entity: Entity dict
        :type entity: dict
        :return: The created model (to be saved)
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises AssertionError: validation errors
        """
        model = self._getCrudHelper().create_model(entity)
        self._save_hook('create', model)
        return model

    def _method_get(self, query_obj=None, *filter, **filter_by):
        """ Fetch a single entity

        :param query_obj: Query Object
        :param filter: Additional filter() criteria
        :param filter_by: Additional filter_by() criteria
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        return self._get_one(query_obj, *filter, **filter_by)

    def _method_replace(self, entity, *filter, **filter_by):
        """ Replace an existing entity

        :param entity: Entity dict
        :type entity: dict
        :param filter: Criteria to find the previous entity
        :param filter_by: Criteria to find the previous entity
        :return: (new model, prev model)
        :rtype: (sqlalchemy.ext.declarative.DeclarativeMeta, sqlalchemy.ext.declarative.DeclarativeMeta)
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        prev_model = self._get_one(None, *filter, **filter_by)
        new_model = self._getCrudHelper().replace_model(entity, prev_model)

        self._save_hook('replace', new_model, prev_model)
        return new_model, prev_model

    def _method_update(self, entity, *filter, **filter_by):
        """ Update an existing entity by merging the fields

        :param entity: Entity dict
        :type entity: dict
        :param filter: Criteria to find the previous entity
        :param filter_by: Criteria to find the previous entity
        :return: The updated model (to be saved)
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        prev_model = self._get_one(None, *filter, **filter_by)
        new_model = self._getCrudHelper().update_model(prev_model, entity)

        self._save_hook('update', new_model, prev_model)
        return new_model

    def _method_delete(self, *filter, **filter_by):
        """ Delete an existing entity

        Loads the entity prior to deletion.

        :param filter: Criteria to find the previous entity
        :param filter_by: Criteria to find the previous entity
        :return: The model to be deleted
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        return self._get_one(None, *filter, **filter_by)
