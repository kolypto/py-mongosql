from sqlalchemy.orm import Query, Load, defaultload, undefer
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

    def __init__(self, model, query=None, _as_relation=None, join_path=None, aliased=None):
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
        if query is None:
            query = Query([model.model])
        self.join_hook = None
        self._model = model
        # This magic is here because if we use alias as target_model it
        # somehow override the Model class. So tests fails if run all tests,
        # but if you run single test - it pass. So we create MongoModel for alias
        # here instead of "get_for".
        if aliased:
            self._model = MongoModel(aliased)
        self._query = query
        self.join_path = join_path or ()

        if join_path:
            self._as_relation = defaultload(*join_path)
        else:
            self._as_relation = defaultload(_as_relation) if _as_relation else Load(self._model.model)
        self.join_queries = []
        self.skip_or_limit = False
        self._order_by = None
        self._project = {}
        self._end_query = None

    def on_join(self, on_join):
        self.join_hook = on_join

    def get_project(self):
        self.end()
        if not self._project:
            self._project = {name: 1 for name, c in self._model.model_bag.columns.items()}
        return self._project

    def set_project(self, project):
        self._project = project

    def aggregate(self, agg_spec):
        """ Select aggregated results """
        a = self._model.aggregate(agg_spec)
        if a:
            self._query = self._query.with_entities(*a)
            # When no model criteria is specified, like COUNT(*), SqlAlchemy won't set the FROM clause
            # Thus, we need to explicitly set the `FROM` clause in these cases
            if self._query.whereclause is None:
                self._query = self._query.select_from(self._model.model)

        return self

    def project(self, projection):
        """ Apply a projection to the query """
        p, projected_properties = self._model.project(projection, as_relation=self._as_relation)
        if self._model.model.__name__ == 'User':
            assert 1
        self._query = self._query.options(p)
        self._project.update(projected_properties)
        return self

    def sort(self, sort_spec):
        """ Apply sorting to the query """
        s = self._model.sort(sort_spec)
        self._query = self._query.order_by(*s)
        self._order_by = s
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

    def _join(self, relnames, join_func):
        """ Base for join and outerjoin """
        for mjp in self._model.join(relnames, as_relation=self._as_relation, callback=self.join_hook):
            # Complex joins
            if mjp.query is not None:
                if self.skip_or_limit:
                    self.join_queries.append((mjp, join_func))
                    continue
                else:
                    self._add_join_query(mjp, join_func)
                    continue
            # Options or projection
            if mjp.options:
                self._query = self._query.options(*mjp.options)
            else:
                joined = self.get_for(
                    mjp.target_model
                )
                self._project[mjp.relname] = joined.get_project()

        self._query = self._query.with_labels()
        return self

    def join(self, relnames):
        """ Use join when there is queries on relations,
        When there no 'project' or other queries on relations
        use .joinedload(rel)
        """
        return self._join(relnames, 'join')

    def outerjoin(self, relnames):
        """ Use outerjoin when there is queries on relations"""
        return self._join(relnames, 'outerjoin')

    def count(self):
        """ Count rows instead """
        self._query = self._query.from_self(func.count(1))
        self.join(())  # no relationships should be loaded
        return self

    def query(self, project=None, sort=None, group=None, filter=None, skip=None, limit=None, join=None, aggregate=None, count=False, outerjoin=None, **__unk):
        """ Build a query
        :param project: Projection spec
        :param sort: Sorting spec
        :param group: Grouping spec
        :param filter: Filter criteria
        :param skip: Skip rows
        :param limit: Limit rows
        :param join: Eagerly load relations
        :param outerjoin: Eagerly load relations use LEFT OUTER JOIN
        :param aggregate: Select aggregated results
        :param count: True to count rows instead
        :raises AssertionError: unknown Query Object operations provided (extra keys)
        :rtype: MongoQuery
        """
        assert not __unk, 'Unknown Query Object operations: {}'.format(__unk.keys())
        q = self
        self.skip_or_limit = skip or limit
        if join:            q = q.join(join)
        if outerjoin:       q = q.outerjoin(outerjoin)
        if project:         q = q.project(project)
        if aggregate:       q = q.aggregate(aggregate)
        if filter:          q = q.filter(filter)
        if sort:            q = q.sort(sort)
        if group:           q = q.group(group)
        if skip or limit:   q = q.limit(limit, skip)
        return q.count() if count else q

    def _add_join_query(self, mjp, join_func):
        model_alias = mjp.rel_alias
        query_with_joined = getattr(self._query, join_func)(model_alias, mjp.relationship)
        if mjp.additional_filter:
            query_with_joined = mjp.additional_filter(query_with_joined)

        for_join = self.get_for(
            mjp.target_model,
            query_with_joined,
            _as_relation=mjp.relationship,
            join_path=self.join_path + (mjp.relationship, ),
            aliased=model_alias
        )
        for_join.on_join(self.join_hook)
        for_join_query = for_join.query(**mjp.query)
        self._project[mjp.relname] = for_join_query.get_project()
        self._query = for_join_query.end()
        join_query = self._query.with_labels()
        self._query = join_query
        self._query = self._query.options(*mjp.options)

    def end(self):
        """ Get the Query object
        :rtype: sqlalchemy.orm.Query
        """
        if self._end_query is not None:
            return self._end_query

        if self.join_queries:
            if self._order_by:
                self._query = self._query.options(*[undefer(x.key or x.element.key) for x in self._order_by])
            self._query = self._query.from_self()
            for mjp, join_func in self.join_queries:
                self._add_join_query(mjp, join_func)
            # Apply order to the resulting query
            if self._order_by is not None:
                self._query = self._query.order_by(*self._order_by)
        self._end_query = self._query
        return self._end_query
