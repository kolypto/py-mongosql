from .model import MongoModel
from .query import MongoQuery


class MongoSqlBase(object):
    """ Mixin for SqlAlchemy models

        Provides methods for accessing :cls:MongoModel and :cls:MongoQuery
    """

    __mongomodel = None

    @classmethod
    def mongomodel(cls):
        """ Get MongoModel object
        :rtype: mongosql.MongoModel
        """
        if cls.__mongomodel is None:
            cls.__mongomodel = MongoModel(cls)
        return cls.__mongomodel

    @classmethod
    def mongoquery(cls, query=None):
        """ Build a MongoQuery
        :param query: Query to start with
        :type query: sqlalchemy.orm.Query
        :rtype: mongosql.MongoQuery
        """
        if query is None:
            query = cls.query  # Requires Base.query = Session.query_property()
        return MongoQuery(cls.mongomodel(), query)
