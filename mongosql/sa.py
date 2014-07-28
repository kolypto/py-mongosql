from .model import MongoModel
from .query import MongoQuery


class MongoSqlBase(object):
    """ Mixin for SqlAlchemy models

        Provides methods for accessing :cls:MongoModel and :cls:MongoQuery
    """

    __mongomodel = None

    @property
    def mongomodel(self):
        """ Get MongoModel object
        :rtype: mongosql.MongoModel
        """
        if self.__mongomodel is None:
            self.__mongomodel = MongoModel(self)
        return self.__mongomodel

    def mongoquery(self, query):
        """ Build a MongoQuery
        :param query: Query to start with
        :type query: sqlalchemy.orm.Query
        :rtype: mongosql.MongoQuery
        """
        return MongoQuery(self.mongomodel, query)
