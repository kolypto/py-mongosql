from __future__ import absolute_import

from sqlalchemy.orm import Query, Load, defaultload, undefer
from sqlalchemy.sql import func

from .bag import ModelPropertyBags
from . import statements
from .utils import outer_with_filter


class JoinedQuery(object):
    def __init__(self, mjp, join_func):
        """

        :type mjp: mongosql.statements.MongoJoinParams
        :param join_func:
        """
        self.mjp = mjp
        self.relname = mjp.relationship_name
        self.join_func = join_func
        self.query = None
        self.processed = False

    def has_filter(self):
        if self.mjp.query_object is None:
            return False
        return bool('filter' in self.mjp.query_object)

    @classmethod
    def from_mjp(cls, mjp, join_func):
        joined = cls(mjp, join_func)
        if mjp.query_object is None:
            joined.query = MongoQuery(mjp.target_model)
            joined.processed = True
        return joined

    def apply(self, parent_query):
        """

        :type parent_query: MongoQuery
        """
        if self.processed:
            return parent_query
        mjp = self.mjp
        join_func = self.join_func
        model_alias = mjp.target_model_aliased
        if join_func == 'outerjoin' and mjp.query_object and 'filter' in mjp.query_object:
            outer_filter = mjp.query_object.pop('filter')
            c = statements.MongoFilter(model_alias).input(outer_filter).compile_statement()
            query_with_joined = outer_with_filter(parent_query._query, model_alias, mjp.relationship, c)
        else:
            query_with_joined = getattr(parent_query._query, join_func)(model_alias, mjp.relationship)
        if mjp.additional_filter:
            query_with_joined = mjp.additional_filter(query_with_joined)

        for_join = MongoQuery(
            model_alias,  # Use an alias in the query
            query_with_joined,
            _join_path=parent_query._join_path + (mjp.relationship, )
        )
        for_join_query = for_join.query(**mjp.query_object)
        self.query = for_join_query
        parent_query._query = for_join_query.end()
        join_query = parent_query._query.with_labels()
        parent_query._query = join_query
        parent_query._query = parent_query._query.options(*mjp.options)
        self.processed = True
        return parent_query

    def apply_filter(self, parent):
        if self.join_func == 'outerjoin':
            return parent
        mjp = self.mjp
        filter_for_parent = {mjp.relationship_name + '.' + key: val for key, val in mjp.query_object['filter'].items()}
        parent = parent.filter(filter_for_parent)
        return parent


class MongoQuery(object):
    """ MongoDB-style queries """

    def __init__(self, model, query=None, _join_path=()):
        """ Init a MongoDB-style query

        :param model: SqlAlchemy model to make a MongoSQL query for, or an alias
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta | sqlalchemy.orm.util.AliasedClass
        :param query: Initial Query to work with
        :type query: sqlalchemy.orm.Query | None
        :param _as_relation: Parent relationship.
            Internal argument used when working with deeper relations:
            is used as initial path for defaultload(_as_relation).lazyload(...).
        :type _as_relation: sqlalchemy.orm.relationships.RelationshipProperty | None
        :param join_path: A tuple of relationships leading to this query.
            This internal argument is used when working with deeper relations, and is used as
            initial path for defaultload(*_join_path).lazyload(...)
        :type join_path: tuple[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        self._model = model
        self._model_bags = ModelPropertyBags.for_model(self._model)
        self._query = query or Query([model])

        self._join_path = _join_path
        self._as_relation = defaultload(*self._join_path) if self._join_path else Load(self._model)

        # Initialize properties that will be used while processing a Query object
        self.join_queries = []  # List of joined queries (processed in end())
        self.skip_or_limit = False  # whether a `skip` or a `limit` is present
        self._order_by = None
        self._project = {}
        self._end_query = None  # The final query, to make sure that end() is only called once

    def get_project(self):
        self.end()
        if all([isinstance(x, dict) for x in self._project.values()]):
            self._project.update({name: 1 for name, c in self._model_bags.columns})
        for joined in self.join_queries:
            if joined.query:
                self._project[joined.relname] = joined.query.get_project()
        return self._project

    def set_project(self, project):
        self._project = project

    def aggregate(self, agg_spec):
        """ Select aggregated results """
        a = statements.MongoAggregateInsecure(
            self._model,
            statements.MongoFilter(self._model)
        ).input(agg_spec).compile_statements()
        if a:
            self._query = self._query.with_entities(*a)
            # When no model criteria is specified, like COUNT(*), SqlAlchemy won't set the FROM clause
            # Thus, we need to explicitly set the `FROM` clause in these cases
            if self._query.whereclause is None:
                self._query = self._query.select_from(self._model)

        return self

    def project(self, projection):
        """ Apply a projection to the query """
        mp = statements.MongoProjection(self._model).input(projection)
        p = mp.compile_options(as_relation=self._as_relation)

        # this code has weird requirements
        # for now, I just make sure they're met
        if mp.mode == mp.MODE_INCLUDE:
            projected_properties = mp.projection.copy()
        else:
            projected_properties = mp.get_full_projection()

        self._query = self._query.options(p)
        self._project.update(projected_properties)
        return self

    def sort(self, sort_spec):
        """ Apply sorting to the query """
        s = statements.MongoSort(self._model).input(sort_spec).compile_columns()
        self._query = self._query.order_by(*s)
        self._order_by = s
        return self

    def group(self, group_spec):
        """ Apply grouping to the query """
        g = statements.MongoSort(self._model).input(group_spec).compile_columns()
        self._query = self._query.group_by(*g)
        return self

    def filter(self, criteria):
        """ Add criteria to the query """
        c = statements.MongoFilter(self._model).input(criteria).compile_statement()
        self._query = self._query.filter(c)
        return self

    def limit(self, limit=None, skip=None, force=False):
        """ Slice results """
        assert skip is None or isinstance(skip, int), 'Skip must be one of: None, int'
        assert limit is None or isinstance(limit, int), 'Limit must be one of: None, int'
        skip = None if skip is None or skip <= 0 else skip
        limit = None if limit is None or limit <= 0 else limit

        if not force and any([j.has_filter() for j in self.join_queries]):
            self.skip_or_limit = (skip, limit)
            return self

        if skip:
            self._query = self._query.offset(skip)
        if limit:
            self._query = self._query.limit(limit)
        return self

    def _join(self, relnames, join_func):
        """ Base for join and outerjoin """
        for mjp in statements.MongoJoin(self._model).input(relnames).compile_options(as_relation=self._as_relation):
            self.join_queries.append(JoinedQuery.from_mjp(mjp, join_func))
        self._query = self._query.options([self._as_relation.lazyload('*')])
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
        self._query = self.end(count=True)
        self._end_query = self._query.from_self(func.count(1))
        self.join(())
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
        if not count and sort:            q = q.sort(sort)
        if group:           q = q.group(group)
        if skip or limit:   q = q.limit(limit, skip)
        return q.count() if count else q

    def end(self, count=False):
        """ Get the Query object
        :rtype: sqlalchemy.orm.Query
        """
        if self._end_query is not None:
            return self._end_query
        if count and self.join_queries:
            if any([j.has_filter() for j in self.join_queries]):
                for joined_query in self.join_queries:
                    if joined_query.has_filter():
                        self = joined_query.apply_filter(self)
        if self.join_queries and not count:
            if self._order_by:
                self._query = self._query.options(*[undefer(x.key or x.element.key) for x in self._order_by])
            if self.skip_or_limit:
                if any([j.has_filter() for j in self.join_queries]):
                    for joined_query in self.join_queries:
                        if joined_query.has_filter():
                            self = joined_query.apply_filter(self)
                    skip, limit = self.skip_or_limit
                    self = self.limit(limit, skip, force=True)
                self._query = self._query.from_self()
            for joined_query in self.join_queries:
                self = joined_query.apply(self)
            # Apply order to the resulting query
            if self._order_by is not None:
                self._query = self._query.order_by(*self._order_by)
        self._end_query = self._query
        return self._end_query
