from sqlalchemy.orm import Session

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
    def mongoquery(cls, query, **kwargs):
        """ Build a MongoQuery
        :param query: Query to start with, or a session object to initiate the query with
        :type query: sqlalchemy.orm.Query|sqlalchemy.orm.Session
        :rtype: mongosql.MongoQuery
        """
        if isinstance(query, Session):
            query = query.query(cls)
        return MongoQuery(cls.mongomodel(), query, **kwargs)
