from sqlalchemy.orm import Query, Load, defaultload
from sqlalchemy.orm.query import QueryContext
from sqlalchemy.sql import func

from .model import MongoModel


class MongoQuery(object):
    """ MongoDB-style queries """

    @classmethod
    def get_for(cls, model, *args, **kwargs):
        """ Get MongoQuery for a model.

        Attempts to use `mongoquery` property of the model

        :param model: Model
        :type model: mongosql.MongoSqlBase|sqlalchemy.ext.declarative.DeclarativeMeta
        :rtype: MongoQuery
        """
        try:
            return model.mongoquery(*args, **kwargs)
        except AttributeError:
            return cls(MongoModel.get_for(model), *args, **kwargs)

    def __init__(self, model, query, _as_relation=None):
        """ Init a MongoDB-style query
        :param model: MongoModel
        :type model: mongosql.MongoModel
        :param query: Query to work with
        :type query: sqlalchemy.orm.Query
        :param _as_relation: Parent relationship.
            Internal argument used when working with deeper relations:
            is used as initial path for defaultload(_as_relation).lazyload(...).
        :type _as_relation: sqlalchemy.orm.relationships.RelationshipProperty
        """
        assert isinstance(model, MongoModel)
        assert isinstance(query, Query)

        self._model = model
        self._query = query
        self._as_relation = defaultload(_as_relation) if _as_relation else Load(self._model.model)

        self._no_joindefaults = False

    def aggregate(self, agg_spec):
        """ Select aggregated results """
        a = self._model.aggregate(agg_spec)
        if a:
            self._query = self._query.with_entities(*a)
            self._no_joindefaults = True  # no relationships should be loaded

            # When no model criteria is specified, like COUNT(*), SqlAlchemy won't set the FROM clause
            # Thus, we need to explicitly set the `FROM` clause in these cases
            if self._query.whereclause is None:
                self._query = self._query.select_from(self._model.model)

        return self

    def project(self, projection):
        """ Apply a projection to the query """
        p = self._model.project(projection, as_relation=self._as_relation)
        if self._model.model.__name__ == 'User':
            assert 1
        self._query = self._query.options(p)
        return self

    def sort(self, sort_spec):
        """ Apply sorting to the query """
        s = self._model.sort(sort_spec)
        self._query = self._query.order_by(*s)
        return self

    def group(self, group_spec):
        """ Apply grouping to the query """
        g = self._model.group(group_spec)
        self._query = self._query.group_by(*g)
        return self

    def filter(self, criteria):
        """ Add criteria to the query """
        c = self._model.filter(criteria)
        self._query = self._query.filter(c)
        return self

    def limit(self, limit=None, skip=None):
        """ Slice results """
        limit, skip = self._model.limit(limit, skip)
        if skip:
            self._query = self._query.offset(skip)
        if limit:
            self._query = self._query.limit(limit)
        return self

    def join(self, relnames):
        """ Eagerly load relations """
        for mjp in self._model.join(relnames, as_relation=self._as_relation):
            # Complex joins
            if mjp.query:
                self._query = self.get_for(
                        mjp.target_model,
                        self._query.join(mjp.relationship),
                        _as_relation=mjp.relationship
                    )\
                    .query(**mjp.query)\
                    .end()

            # Options
            self._query = self._query.options(*mjp.options)

        self._query = self._query.with_labels()
        self._no_joindefaults = True
        return self

    def count(self):
        """ Count rows instead """
        self._query = self._query.from_self(func.count(1))
        self._no_joindefaults = True  # no relationships should be loaded
        return self

    def query(self, project=None, sort=None, group=None, filter=None, skip=None, limit=None, join=None, aggregate=None, count=False, **__unk):
        """ Build a query
        :param project: Projection spec
        :param sort: Sorting spec
        :param group: Grouping spec
        :param filter: Filter criteria
        :param skip: Skip rows
        :param limit: Limit rows
        :param join: Eagerly load relations
        :param aggregate: Select aggregated results
        :param count: True to count rows instead
        :raises AssertionError: unknown Query Object operations provided (extra keys)
        :rtype: MongoQuery
        """
        assert not __unk, 'Unknown Query Object operations: {}'.format(__unk.keys())

        q = self
        if join:            q = q.join(join)
        if project:         q = q.project(project)
        if aggregate:       q = q.aggregate(aggregate)
        if filter:          q = q.filter(filter)
        if sort:            q = q.sort(sort)
        if group:           q = q.group(group)
        if skip or limit:   q = q.limit(limit, skip)
        return q.count() if count else q

    def end(self):
        """ Get the Query object
        :rtype: sqlalchemy.orm.Query
        """
        if not self._no_joindefaults:
            self.join(())  # have to join with an empty list explicitly so all relations get noload()
        return self._query
