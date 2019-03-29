from __future__ import absolute_import

from copy import copy
from sqlalchemy.orm import Session, Query

from .query import MongoQuery


class MongoSqlBase(object):
    """ Mixin for SqlAlchemy models that provides the .mongoquery() method for convenience """

    # Override this method in your subclass in order to be able to configure MongoSql on a per-model basis!
    @classmethod
    def _init_mongoquery(cls, handler_settings=None):
        """ Get a reusable MongoQuery object. Is only invoked once.

            Override this method in order to initialize MongoQuery they way you need.
            For example, you might want to pass `handler_settings` dict to it.

            But for now, you can only use `mongoquery_configure()` on it.

            :rtype: MongoQuery
        """

        # Idea: have an `_api` field in your models that will feed MongoQuery with the settings
        # Example: return MongoQuery(cls, handler_settings=cls._api)

        return MongoQuery(cls, handler_settings=handler_settings)

    @classmethod
    def _get_mongoquery(cls):
        """ Get a Reusable MongoQuery for this model ; initialize it only once

            :rtype: MongoQuery
        """
        # Initialize cached property
        # We check __dict__, because getattr() would look up all parent classes,
        # but we only need our own, class-local MongoQuery
        if '__cached_mongoquery' not in cls.__dict__:
            cls.__cached_mongoquery = cls._init_mongoquery()

        # Return a copy
        return copy(cls.__cached_mongoquery)

    @classmethod
    def mongoquery_configure(cls, handler_settings):
        """ Initialize this models' MongoQuery settings and make it permanent.

            This method is just a shortcut to do configuration the lazy way.
            A better way would be to subclass MongoSqlBase and override the _init_mongoquery() method.
            See _init_mongoquery() for a suggestion on how to do this.

            :param handler_settings: a dict of settings. See MongoQuery
        """
        # Initialize a configured MongoQuery
        mq = cls._init_mongoquery(handler_settings)

        # Put it in cache
        cls.__cached_mongoquery = mq

        # Done
        return mq

    @classmethod
    def mongoquery(cls, query_or_session=None):
        """ Build a MongoQuery

        Note that when `None` is given, the resulting Query is not bound to any session!
        You'll have to bind it manually, after calling .end()

        :param query_or_session: Query to start with, or a session object to initiate the query with
        :type query_or_session: sqlalchemy.orm.Query | sqlalchemy.orm.Session | None
        :rtype: mongosql.MongoQuery
        """
        if query_or_session is None:
            query = Query([cls])
        elif isinstance(query_or_session, Session):
            query = query_or_session.query(cls)
        elif isinstance(query_or_session, Query):
            query = query_or_session
        else:
            raise ValueError('Argument must be Query or Session')

        return cls._get_mongoquery().from_query(query)
