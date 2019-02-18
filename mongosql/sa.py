from __future__ import absolute_import

from sqlalchemy.orm import Session, Query

from .query import MongoQuery


class MongoSqlBase(object):
    """ Mixin for SqlAlchemy models that provides the .mongoquery() method for convenience """

    @classmethod
    def mongoquery(cls, query_or_session=None, **kwargs):
        """ Build a MongoQuery
        :param query_or_session: Query to start with, or a session object to initiate the query with
        :type query_or_session: sqlalchemy.orm.Query | sqlalchemy.orm.Session
        :rtype: mongosql.MongoQuery
        """
        if isinstance(query_or_session, Session):
            query = query_or_session.query(cls)
        elif isinstance(query_or_session, Query):
            query = query_or_session
        else:
            raise ValueError('Argument must be Query or Session')
        return MongoQuery(cls, query, **kwargs)
