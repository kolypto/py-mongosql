from __future__ import absolute_import
from future.utils import string_types

from copy import deepcopy
import logging
from sqlalchemy.dialects import postgresql as pg

from . import MongoQuery, ModelPropertyBags
from .hist import ModelHistoryProxy
import sys

PY2 = sys.version_info[0] == 2


class CrudHelper(object):
    """ Crud helper functions """

    def __init__(self, model):
        """ Init CRUD helper

        :param model: The model to work with
        :type model: type
        """
        self.model = model
        self.bags = ModelPropertyBags.for_model(model)

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

        mq = MongoQuery(self.model, query)
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
        model_colnames = self.bags.columns.names
        names = set(names)
        return [n for n in names - model_colnames if not isinstance(getattr(self.model, n, None), property)]

    def nullify_empty_fields(self, entity):
        """ Walk through the entity dict and handle nullable fields:

        - If a field has a value of '', set it to None

        :param entity: Entity
        :type entity:
        :return: Altered entity
        :rtype: dict
        """
        for k in self.bags.nullable.keys():
            if k in entity and entity[k] == '':
                entity[k] = None
        return entity

    def create_model(self, entity):
        """ Create an instance from entity dict.

        This only allows to assign column properties and not relations.

        :param entity: Entity dict
        :type entity: dict
        :return: Created instance
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises AssertionError: validation errors
        """
        assert isinstance(entity, dict), 'Create model: entity should be a dict'

        # Check columns
        unk_cols = self.check_columns(entity.keys())
        assert not unk_cols, 'Create model: unknown fields: {}'.format(unk_cols)

        # Create
        return self.model(**entity)

    def update_model(self, entity, instance):
        """ Update an instance from entity dict by merging the fields

        - Properties are copied over
        - JSON dicts are shallowly merged

        :param entity: Entity dict
        :type entity: dict
        :param instance: The instance to update
        :type instance: sqlalchemy.ext.declarative.DeclarativeMeta
        :return: New instance, updated
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises AssertionError: validation errors
        """
        assert isinstance(entity, dict), 'Update model: entity should be a dict'

        # Check columns
        unk_cols = self.check_columns(entity.keys())
        assert not unk_cols, 'Update model: unknown fields: {}'.format(unk_cols)

        # Update
        for name, val in entity.items():
            if isinstance(val, dict) and self.bags.columns.is_column_json(name):
                # JSON column with a dict: Make a copy that can replace the original attribute,
                # so SqlAlchemy history will notice the changes.
                tmp = deepcopy(getattr(instance, name))

                # Do a shallow merge.
                tmp.update(val)
                val = tmp

            setattr(instance, name, val)

        # Finish
        return instance


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
        :param ro_fields: List of read-only properties.

            Also can be a callable which decides on the read-only properties at runtime.

        :type ro_fields: Iterable[str|sqlalchemy.Column|sqlalchemy.orm.properties.ColumnProperty]|Callable
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

        self._ro_fields = ro_fields if callable(ro_fields) else set(c if isinstance(c, string_types) else c.key for c in ro_fields)
        self._allowed_relations = set(c if isinstance(c, string_types) else c.key for c in allow_relations)
        self._query_defaults = query_defaults or {}
        self._maxitems = maxitems or None

        assert callable(self._ro_fields) or all(isinstance(x, string_types) for x in self._ro_fields), 'Some values in `ro_fields` were not converted to string'
        assert all(isinstance(x, str) for x in self._allowed_relations), 'Some values in `allowed_relations` were not converted to string'
        assert isinstance(self._query_defaults, dict), '`query_defaults` was not a dict'
        assert self._maxitems is None or isinstance(self._maxitems, int), '`maxitems` must be an integer'

    @property
    def ro_fields(self):
        """ Get the set of read-only property names

        :rtype: set[str]
        """
        return set(self._ro_fields()) if callable(self._ro_fields) else self._ro_fields

    @property
    def allowed_relations(self):
        """ Get the set of relations that are allowed to join to

        :rtype: set[str]
        """
        return set(self._allowed_relations)

    @classmethod
    def _check_relations(cls, allowed_relations, qo, _prefix=''):
        """ Test Query Object joins against `allowed_relations`, supporting dot-notation

        :param allowed_relations: Set of allowed relations
        :type allowed_relations: set
        :param qo: Query Object
        :type qo: dict | None
        :returns: Banned relationships
        :rtype: set[str]
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
            query_obj = dict(list(self._query_defaults.items()) + (list(query_obj.items()) if query_obj else []))

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
        for k in set(entity.keys()) & self.ro_fields:
            entity.pop(k)

        # Super
        return super(StrictCrudHelper, self).create_model(entity)

    def update_model(self, entity, instance):
        assert isinstance(entity, dict), 'Update model: entity should be a dict'

        # Remove ro fields
        for k in set(entity.keys()) & self.ro_fields:
            entity.pop(k)

        # Super
        return super(StrictCrudHelper, self).update_model(entity, instance)


class CrudViewMixin(object):
    """ Base class for CRUD implementations """

    #: Set the CRUD helper object
    crudhelper = None

    def __init__(self):
        self.sqlaclhemy_queries = []

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
        :rtype: sqlalchemy.orm.Query, list of fields
        """
        mongo_query = self._getCrudHelper().mquery(
            self._query().filter(*filter).filter_by(**filter_by),
            query_obj
        )
        sqlalchemy_query = mongo_query.end()
        try:
            dialect = pg.dialect()
            sql_query = sqlalchemy_query.statement.compile(dialect=dialect)
            if PY2:
                sql_str = (sql_query.string.encode(dialect.encoding) % sql_query.params).decode(dialect.encoding)
            else:
                sql_str = sql_query.string % sql_query.params
            self.sqlaclhemy_queries.append(sql_str)
        except Exception as e:
            logging.error('Error generate SQL string %e', e)
        return sqlalchemy_query, mongo_query.get_project()

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
        sql_query, projection = self._mquery(query_obj, *filter, **filter_by)

        instance = sql_query.one()
        return instance, projection

    def _save_hook(self, new, prev=None):
        """ Hook into create(), update() methods.

        This allows to make some changes to the instance before it's actually saved.

        :param new: New version
        :type new: sqlalchemy.ext.declarative.DeclarativeMeta
        :param prev: Previously persisted version (only when updating).
        :type prev: mongosql.hist.ModelHistoryProxy
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
        sql_query, projection = self._mquery(query_obj, *filter, **filter_by)
        res = sql_query.all()

        # Count?
        if query_obj and query_obj.get('count', 0):
            return res[0][0], None  # Scalar count query

        # Convert KeyedTuples to dicts (when aggregating)
        if query_obj and 'aggregate' in query_obj:
            return [dict(zip(row.keys(), row)) for row in res], None
        return res, projection

    def _method_create(self, entity):
        """ Create a new entity

        :param entity: Entity dict
        :type entity: dict
        :return: The created instance (to be saved)
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises AssertionError: validation errors
        """
        instance = self._getCrudHelper().create_model(entity)
        self._save_hook(instance)
        return instance

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

    def _method_update(self, entity, *filter, **filter_by):
        """ Update an existing entity by merging the fields

        :param entity: Entity dict
        :type entity: dict
        :param filter: Criteria to find the previous entity
        :param filter_by: Criteria to find the previous entity
        :return: The updated instance (to be saved)
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        instance, _ = self._get_one(None, *filter, **filter_by)
        instance = self._getCrudHelper().update_model(entity, instance)
        self._save_hook(
            instance,
            ModelHistoryProxy(instance)
        )
        return instance

    def _method_delete(self, *filter, **filter_by):
        """ Delete an existing entity

        Loads the entity prior to deletion.

        :param filter: Criteria to find the previous entity
        :param filter_by: Criteria to find the previous entity
        :return: The instance to be deleted
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
        :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        :raises AssertionError: validation errors
        """
        item, _ = self._get_one(None, *filter, **filter_by)
        return item
