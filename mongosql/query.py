from sqlalchemy.orm import Query
from sqlalchemy.sql import func

from .model import MongoModel


class MongoQuery(object):
    """ MongoDB-style queries """

    def __init__(self, model, query):
        """ Init a MongoDB-style query
        :param model: MongoModel
        :type model: mongosql.MongoModel
        :param query: Query to work with
        :type query: sqlalchemy.orm.Query
        """
        assert isinstance(model, MongoModel)
        assert isinstance(query, Query)

        self._model = model
        self._query = query

        self._joined = False

    def project(self, projection):
        """ Apply a projection to the query """
        p = self._model.project(projection)
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
        j = self._model.join(relnames)
        self._query = self._query.options(*j).with_labels()
        self._joined = True
        return self

    def count(self):
        """ Count rows instead """
        self._query = self._query.from_self(func.count(1))
        self._joined = True  # no relationships should be loaded
        return self

    def query(self, project=None, sort=None, group=None, filter=None, skip=None, limit=None, join=None, count=False):
        """ Build a query
        :param project: Projection spec
        :param sort: Sorting spec
        :param group: Grouping spec
        :param filter: Filter criteria
        :param skip: Skip rows
        :param limit: Limit rows
        :param join: Eagerly load relations
        :param count: True to count rows instead
        """
        self.project(project).sort(sort).group(group).filter(filter).limit(limit, skip).join(join)
        return self.count() if count else self

    def end(self):
        """ Get the Query object
        :rtype: sqlalchemy.orm.Query
        """
        if not self._joined:
            self.join(())  # have to join with an empty list explicitly so all relations get noload()
        return self._query
