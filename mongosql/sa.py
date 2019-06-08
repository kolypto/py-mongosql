from copy import copy
from typing import Union

from sqlalchemy.orm import Session, Query

from .query import MongoQuery


class MongoSqlBase:
    """ Mixin for SqlAlchemy models that provides the .mongoquery() method for convenience """

    # Override this method in your subclass in order to be able to configure MongoSql on a per-model basis!
    @classmethod
    def _init_mongoquery(cls, handler_settings: dict = None) -> MongoQuery:
        """ Get a reusable MongoQuery object. Is only invoked once.

            Override this method in order to initialize MongoQuery they way you need.
            For example, you might want to pass `handler_settings` dict to it.

            But for now, you can only use `mongoquery_configure()` on it.

            :rtype: MongoQuery
        """

        # Idea: have an `_api` field in your models that will feed MongoQuery with the settings
        # Example: return MongoQuery(cls, handler_settings=cls._api)

        return MongoQuery(cls, handler_settings=handler_settings)

    __mongoquery_per_class_cache = {}

    @classmethod
    def _get_mongoquery(cls) -> MongoQuery:
        """ Get a Reusable MongoQuery for this model ; initialize it only once

            :rtype: MongoQuery
        """
        try:
            # We want ever model class to have its own MongoQuery,
            # and we want no one to inherit it.
            # We could use model.__dict__ for this, but classes in Python 3 use an immutable `mappingproxy` instead.
            # Thus, we have to keep our own cache of ModelPropertyBags.
            mq = cls.__mongoquery_per_class_cache[cls]
        except KeyError:
            cls.__mongoquery_per_class_cache[cls] = mq = cls._init_mongoquery()

        # Return a copy
        return copy(mq)

    @classmethod
    def mongoquery_configure(cls, handler_settings: dict) -> MongoQuery:
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
    def mongoquery(cls, query_or_session: Union[Query, Session] = None) -> MongoQuery:
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
