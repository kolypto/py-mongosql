from __future__ import absolute_import

from copy import copy

from sqlalchemy.orm import Query, Load, defaultload

from .bag import ModelPropertyBags
from . import handlers
from .exc import InvalidQueryError
from .util import QuerySettings


class MongoQuery(object):
    """ MongoDB-style queries """

    def __init__(self, model, handler_settings=None):
        """ Init a MongoDB-style query

        :param model: SqlAlchemy model to make a MongoSQL query for.
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        :param handler_settings: Settings for Query Object handlers.
            These are just plain kwargs names for every handler object's __init__ method.
            Address the relevant documentation.
            Note that you don't have to specify which object receives which kwarg:
            the `QuerySettings` object does that automatically.

            To disable a handler, give its name mapped to a `False`.
            Example:

                project=False

            A special key, `related`, lets you specify the settings for queries on related models.
            For example, a MongoQuery(Article) can specify settings for queries made with joins to a related User model:

                related={'author': { default_exclude=('password',) } }

            The list of all settings:
                # projection
                    default_projection=None
                    default_exclude=None
                    force_include=None
                    force_exclude=None
                # projection & join & joinf
                    raiseload=False
                # aggregate
                    aggregateable_columns=()
                    aggregate_labels=False
                # filter
                    scalar_operators=None
                    array_operators=None
                # join & joinf
                    allowed_relations=None
                    banned_relations=None
                # limit
                    max_limit=None
                # enabled handlers?
                    project=True
                    filter=True
                    join=True
                    joinf=True
                    group=True
                    sort=True
                    aggregate=True
                    limit=True
                # Settings for queries on related models
                    related = dict(
                        relation-name: dict
                        relation-name: lambda: dict
                    )

        :type handler_settings: dict | None
        """
        self._model = model  # model, or its alias (when used with self.aliased())
        self._bags = ModelPropertyBags.for_model(self._model)

        # Get the settings
        handler_settings = handler_settings or {}
        if handler_settings.get('join', True) is False:
            # If 'join' is explicitly disabled, disable 'joinf' as well
            # This is for security so that one doesn't forget to disable them both.
            handler_settings['joinf'] = False
        self._handler_settings = QuerySettings(handler_settings)

        # Initialized later
        self._query = None  # type: Query | None

        # Get ready: Query object handlers
        self._init_query_object_handlers()

        # Load interface join path
        # These are just the defaults ; as_relation() will override them when working with
        # deeper relationships
        self._join_path = ()
        self._as_relation = Load(self._model)

        # Cached MongoQuery objects for nested relationships
        self._nested_mongoqueries = dict()  # type: dict[str, MongoQuery]

        # NOTE: keep in mind that this object is copy()ed in order to make it reusable.
        # This means that every property that can't be safely reused has to be copy()ied manually
        # inside the __copy__() method.
        # A good example is the `_as_relation()` method: if not copied properly, subsequent queries
        # will inherit all option()s from the previous queries and lead to all sorts of weird effects!
        # So, whenever you add a property to this object, make sure you understand its copy() behavior.

    def __copy__(self):
        """ MongoQuery can be reused: wrap it with Reusable() which performs the automatic copy()

            It actually makes sense to have reusable MongoQuery because there's plenty of settings
            you don't want to parse over ang over again.

            This method implements proper copying so that this MongoQuery can be reused.
        """
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)

        # Copy Query Object handlers
        for name in self.HANDLER_ATTR_NAMES:
            setattr(result, name, copy(getattr(result, name)))

        # Re-initialize properties that can't be copied
        self.as_relation(None)  # reset the Load() interface. Outside code will have to set it up properly
        self._query = None

        return result

    def from_query(self, query):
        """ Specify a custom sqlalchemy query to work with.

        It can have, say, initial filtering already applied to it.
        It no default query is provided, _from_query() will use the default.

        :param query: Initial sqlalchemy query to work with (e.g. with initial filters pre-applied)
        :type query: sqlalchemy.orm.Query
        """
        self._query = query
        return self

    def as_relation(self, join_path=None):
        """ Handle a model in relationship with another model

            This internal method is used when working with deeper relations.
            For example, when you're querying `User`, who has `User.articles`, and you want to specify lazyload() on
            the fields of that article, you can't just do `lazyload(User.articles)` ; you have to tell sqlalchemy that
            you actually mean a model that is going to be loaded through a relationship.
            You do it this way:

                defaultload(models.User.articles).lazyload(models.Article)

            Then SqlAlchemy will know that you actually mean a related model.

            To achieve this, we keep track of nested relations in the form of `join_path`.
            `self._as_relation` is the Load() interface for chaining methods for deeper relationships.

            :param join_path: A tuple of relationships leading to this query.
            :type join_path: tuple[sqlalchemy.orm.relationships.RelationshipProperty] | None
        """
        if join_path:
            self._join_path = join_path
            self._as_relation = defaultload(*self._join_path)
        else:
            # Set default
            # This behavior is used by the __copy__() method to reset the attribute
            self._join_path = ()
            self._as_relation = Load(self._model)
        return self

    def aliased(self, model):
        """ Make a query to an aliased model instead.

        This is used by MongoJoin handler to issue subqueries.
        Note that the method modifies the current object and does not make a copy!

        :param model: Aliased model
        """
        # Aliased bags
        self._bags = self._bags.aliased(model)
        self._model = model

        # Aliased loader interface
        # Currently, our join path looks like this: [..., User]
        # Now, when we're using an alias instead, we have to replace that last element with an alias too
        # SqlAlchemy 1.2.x used to work well without doint it;
        # SqlAlchemy 1.3.x now requires adapting a relationship by using of_type() on it.
        # See: https://github.com/sqlalchemy/sqlalchemy/issues/4566
        # Okay. First. Replace the last element on the join path with the aliased model's relationship
        new_join_path = self._join_path[0:-1] + (self._join_path[-1].of_type(model),)
        # Second. Apply the new join path
        self.as_relation(new_join_path)

        # Aliased handlers
        for handler_name in self.HANDLER_ATTR_NAMES:
            setattr(self, handler_name,
                    getattr(self, handler_name).aliased(model))

        return self

    def query(self, **query_object):
        """ Build a MongoSql query from an object

        :param project: Projection spec
        :param sort: Sorting spec
        :param group: Grouping spec
        :param filter: Filter criteria
        :param join: Eagerly load relations, potentially, with a nested query
        :param joinf: Eagerly load relations, potentially, with a nested query that will filter the whole result set
        :param aggregate: Select aggregated results
        :param skip: Skip rows
        :param limit: Limit rows
        :param count: Count the number of rows instead of returning them
        :raises InvalidQueryError: unknown Query Object operations provided (extra keys)
        :raises InvalidQueryError: syntax error for any of the Query Object sections
        :raises InvalidColumnError: Invalid column name provided in the input
        :raises InvalidRelationError: Invalid relationship name provided in the input
        :rtype: MongoQuery
        """
        # Prepare Query Object
        for handler_name, handler in self._handlers():
            query_object = handler.input_prepare_query_object(query_object)

        # Check if Query Object keys are all right
        invalid_keys = set(query_object.keys()) - self.HANDLER_NAMES
        if invalid_keys:
            raise InvalidQueryError(u'Unknown Query Object operations: {}'.format(', '.join(invalid_keys)))

        # Process every field with its method
        # Every handler should be invoked because they may have defaults even when no input was provided
        for handler_name, handler in self._handlers():
            # Query Object value for this handler
            input_value = query_object.get(handler_name, None)

            # Disabled handlers exception
            # But only test that if there actually was any input
            if input_value is not None:
                self._handler_settings.raise_if_not_handler_enabled(self._bags.model_name, handler_name)

            # Use the handler
            # Run it even when it does not have any input
            handler.with_mongoquery(self)
            handler.input(input_value)

        # Done
        return self

    def end(self):
        """ Get the resulting sqlalchemy Query object

        :rtype: sqlalchemy.orm.Query
        """
        # The query
        q = self._from_query()

        # Apply every handler
        for handler_name, handler in self._handlers():
            q = handler.alter_query(q, as_relation=self._as_relation)

        return q

    def pluck_instance(self, instance):
        """ Pluck an sqlalchemy instance and make it into a dict

            This method should be used to prepare an object for JSON encoding.
            This makes sure that only the properties explicitly requested by the user get included
            into the result, and *not* the properties that your code may have loaded.

            Projection and Join properties are considered.

            :param instance: object
            :rtype: dict
        """
        if not isinstance(instance, self._bags.model):  # bags.model, because self.model may be aliased
            raise ValueError('This MongoQuery.pluck_instance() expects {}, but {} was given'
                             .format(self._bags.model, type(instance)))
        # First, projection will do what it wants.
        # By the way, it will also generate a dictionary
        dct = self.handler_project.pluck_instance(instance)
        # Now, the joins will add more fields
        dct.update(self.handler_join.pluck_instance(instance))
        dct.update(self.handler_joinf.pluck_instance(instance))
        # Seems like there's no one else?
        # Done.
        return dct

    # region Query Object handlers

    # This section initializes every Query Object handler, one per method.
    # Doing it this way enables you to override the way they are initialized, and use a custom query class with
    # custom settings.

    _QO_HANDLER_PROJECT = handlers.MongoProject
    _QO_HANDLER_SORT = handlers.MongoSort
    _QO_HANDLER_GROUP = handlers.MongoGroup
    _QO_HANDLER_JOIN = handlers.MongoJoin
    _QO_HANDLER_JOINF = handlers.MongoFilteringJoin
    _QO_HANDLER_FILTER = handlers.MongoFilter
    _QO_HANDLER_AGGREGATE = handlers.MongoAggregate  # Use MongoAggregateInsecure for backwards compatibility
    _QO_HANDLER_LIMIT = handlers.MongoLimit
    _QO_HANDLER_COUNT = handlers.MongoCount

    HANDLER_NAMES = frozenset(('project',
                               'sort',
                               'group',
                               'join',
                               'joinf',
                               'filter',
                               'aggregate',
                               'limit',
                               'count'))
    HANDLER_ATTR_NAMES = frozenset('handler_'+name
                                   for name in HANDLER_NAMES)

    def _handlers(self):
        """ Get the list of all (handler_name, handler) """
        return (
            # Note that the ordering of these handlers may actually influence the way queries are processed!

            # Considerations:
            # 1. 'limit' after 'order_by':
            #    'order_by' does not like limits
            # 2. 'join' after 'filter' and 'limit'
            #    Because 'join' handler may make it into a subquery,
            #    and at that point is has to have all filters and limits applied
            # 3. 'aggregate' before 'sort', 'group', 'filter'
            #    Because aggregate handler uses Query.select_from(), which can only be applied to a query
            #    without any clauses like WHERE, ORDER BY, GROUP BY
            # 4. 'sort' before 'join'
            #    Because join makes a subquery, and it has to contain ordering within it.
            # 5. 'count' after everything
            #    Because it will wrap everything into a subquery, and count the results
            # *. There may be others that the author is not aware of... yet.
            ('project', self.handler_project),
            ('aggregate', self.handler_aggregate),
            ('sort', self.handler_sort),
            ('group', self.handler_group),
            ('filter', self.handler_filter),
            ('limit', self.handler_limit),
            ('join', self.handler_join),
            ('joinf', self.handler_joinf),
            ('count', self.handler_count)
        )

    # for IDE completion
    handler_project = None  # type: mongosql.handlers.MongoProject
    handler_sort = None  # type: mongosql.handlers.MongoSort
    handler_group = None  # type: mongosql.handlers.MongoGroup
    handler_join = None  # type: mongosql.handlers.MongoJoin
    handler_joinf = None  # type: mongosql.handlers.MongoJoinf
    handler_filter = None  # type: mongosql.handlers.MongoFilter
    handler_aggregate = None  # type: mongosql.handlers.MongoAggregate
    handler_limit = None  # type: mongosql.handlers.MongoLimit
    handler_count = None  # type: mongosql.handlers.MongoCount

    def _init_query_object_handlers(self):
        """ Initialize every Query Object handler """
        for name in self.HANDLER_NAMES:
            # Every handler: name, attr, clas
            handler_attr_name = 'handler_' + name
            handler_cls_attr_name = '_QO_HANDLER_' + name.upper()
            handler_cls = getattr(self, handler_cls_attr_name)

            # Use _init_handler()
            setattr(self, handler_attr_name,
                    self._init_handler(name, handler_cls)
                    )

        # Check settings
        self._handler_settings.raise_if_invalid_handler_settings()

    def _init_handler(self, handler_name, handler_cls):
        """ Init a handler, and load its settings """
        handler_settings = self._handler_settings.get_settings(handler_name, handler_cls)
        return handler_cls(self._model, **handler_settings)

    # endregion

    # region Internals

    def _from_query(self):
        """ Get the query to work with, or initialize one

            When the time comes to build an actual SqlAlchemy query, we're going to use the query that the user has
            provided with from_query(). If none was provided, we'll use the default one.
        """
        return self._query or Query([self._model])

    def _init_mongoquery_for_related_model(self, relationship_name):
        """ Create a MongoQuery object for a model, related through a relationship with the given name.

            This method configures queries made on related models.
            Note that this method is only called once for every relationship.

            See: _get_nested_mongoquery() for more info

            :rtype: callable[(), MongoQuery]
        """
        # Get the relationship
        # There must be no exceptions here, because JoinHandler is the only guy using this method,
        # and it should already have validated relationship name.
        # Meaning, we can be pretty sure `relationship_name` exists
        target_model = self._bags.relations.get_target_model(relationship_name)

        # Make a new MongoQuery
        handler_settings = self._handler_settings.settings_for_nested_mongoquery(relationship_name)
        mongoquery = self.__class__(target_model, handler_settings)

        # Done
        return mongoquery

    def _get_nested_mongoquery(self, relationship_name, target_model, target_model_aliased):
        """ Get a MongoQuery for a nested model (through a relationship)

        Remember that the 'join' operation support nested queries!
        And those queries also support projections, filters, joins, and whatnot.
        This method will correctly load nested configuration from self._handler_settings,
        which enables you to set up your security and preferences for queries on related models.

        Example:

        mq = MongoQuery(Comment, dict(
            allowed_relations=('author',),  # only allow one relationship to be joined
            related={
                'author': dict(  # settings for queries on this relationship
                    join=False,  # disable further joins
                    force_exclude=('password',)  # can't get it
                )
            }
        ))

        In this case, the API user won't be able to get the password by join()ing to it from other entities.

        :param target_model:
        :param target_model_aliased:
        :rtype: MongoQuery
        """
        # Get the relationship
        relationship = self._bags.relations[relationship_name]

        # If there's no nested MongoQuery inited, make one
        if relationship_name not in self._nested_mongoqueries:
            self._nested_mongoqueries[relationship_name] = self._init_mongoquery_for_related_model(relationship_name)

        # Get a cached nested MongoQuery
        nested_mq = self._nested_mongoqueries[relationship_name]

        # Make a copy, set as_relation() properly, put an alias on it
        nested_mq = copy(nested_mq) \
            .as_relation(self._join_path + (relationship,)) \
            .aliased(target_model_aliased)

        # Done
        return nested_mq

    # endregion
