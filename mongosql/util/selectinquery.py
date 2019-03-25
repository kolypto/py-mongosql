from sqlalchemy.orm.strategy_options import loader_option, _UnboundLoad
from sqlalchemy.orm.strategies import SelectInLoader
from sqlalchemy.orm import properties
from sqlalchemy import log


@log.class_logger
@properties.RelationshipProperty.strategy_for(lazy="selectin_query")
class SelectInQueryLoader(SelectInLoader):
    """ A custom loader that acts like selectinload(), but supports using a custom query for related models.

    This enables us to use selectinload() with relationships which are loaded with a query that we
    can alter with a callable.

    Example usage:

        selectin_query(
            User.articles,
            lambda q, **kw: \
                q.filter(User.articles.rating > 0.5)
        )
    """

    __slots__ = ('_alter_query', '_bakery')

    def create_row_processor(self, context, path, loadopt, mapper, result, adapter, populators):
        # Pluck the custom callable that alters the query out of the `loadopt`
        self._alter_query = loadopt.local_opts['alter_query']

        # Call super
        super(SelectInQueryLoader, self) \
            .create_row_processor(context, path, loadopt, mapper, result, adapter, populators)

    # The easiest way would be to just copy `SelectInLoader` and make adjustments to the code,
    # but that would require us supporting it, porting every change from SqlAlchemy.
    # We don't want that!
    # Therefore, this class is hacky, and tries to reuse as much code as possible.

    # The main method that performs all the magic is SelectInLoader._load_for_path()
    # I don't want to copy it.
    # Solution? Let's hack into it.

    # The first step is to investigate the way the query is generated.
    # 1. q = self._bakery(lambda session: session.query(...)) makes the Query
    # 2. q.add_criteria(lambda q: ...) is used to alter the query, and add all criteria
    # 3. q.add_criteria(lambda q: q.filter(in_expr.in_....)) builds the IN clause
    # 4. q._add_lazyload_options() copies some options from the original query (`context.query`) with
    # 5. (sometimes) q.add_criteria(lambda q: q.order_by()) orders by a foreign key
    # 6. In a loop, q(context.session).params(..) is invoked, for every chunk

    # Looks like we can wrap self._bakery with a wrapper which will make the query,
    # and inject our alter_query() just before it is used, in step 6.
    # How do we do it?
    # See WrappedBakedQuery

    # noinspection PyMethodOverriding
    def _load_for_path(self, context, path, states, load_only, effective_entity):
        """
        I don't know much about that SqlAlchemy stuff, but at least, here are the types or arguments,
        for the case when we join User.articles:

        :type context: sqlalchemy.orm.query.QueryContext
        :type path: EntityRegistry((User,...))
        :type states: list[(sqlalchemy.orm.state.InstanceState, True)]
        :type load_only: None
        :type effective_entity: Article.mapper
        """
        def alter_query(query):
            """ Wrapper that will call the lambda with all the arguments as keywords """
            return self._alter_query(query,
                                     # We use keyword arguments because sqlalchemy might change the signature,
                                     # and we'll be able to follow
                                     selectinquery=self,  # type: SelectInQueryLoader
                                     context=context,  # type: QueryContext
                                     path=path,  # type: EntityRegistry
                                     states=states,  # type: list[InstanceState]
                                     load_only=load_only,  # type: EntityRegistry
                                     effective_entity=effective_entity  # type: Mapper
                                     )

        # noinspection PyUnreachableCode
        # Replace the old bakery with a new one, which will inject alter_query() just before it gets executed
        bakery_backup = self._bakery
        if True:
            # This one has better performance
            self._bakery = AlteringBakedQuery.Bakery(self._bakery, alter_query)
        else:
            # This one is more straightforward. Use it when debugging isues with selectinquery()
            self._bakery = UnBakedQuery.Bakery(self._bakery, alter_query)

        # Execute the function
        try:
            return super(SelectInQueryLoader, self) \
                ._load_for_path(context, path, states, load_only, effective_entity)
        finally:
            # Un-wrap the bakery
            # If we don't, it will get re-wrapped every time, and we'll ultimately hit recursion limit.
            self._bakery = bakery_backup


# region Bakery Wrappers that will apply alter_query() in the end

from sqlalchemy.ext.baked import BakedQuery


# Here are two BakedQuery wrappers
#
# AlteringBakedQuery will use the cache, but alter the query on the very last step.
# UnBakedQuery will not use the cache at all.
#
# AlteringBakedQuery is supposed to be more performant, but might have bugs
# UnBakedQuery is debugger-friendly
#
# You can use either.
# I use UnBakedQuery when there are issues and I have to debug SelectInQueryLoader;
# I use AlteringQuery when everything seems to work fine

class AlteringBakedQuery(object):
    """ A BakedQuery that will apply `alter_query` callable just before it's executed,
        and still use the cache """

    @classmethod
    def Bakery(cls, bakery, alter_query):
        """ A Bakery that will actually use wrapped BakedQuery objects

        This bakery will keep quiet until the query is executed, and then disable caching,
        apply `alter_query`, and run the altered query.

        :param bakery: The wrapped bakery
        :param alter_query: Callable that will alter the query
        :return:
        """
        return lambda initial_fn, *cache_args: \
            cls(bakery(initial_fn, *cache_args), alter_query)

    __slots__ = ('_baked_query', '_alter_query', '_done_once')

    def __init__(self, baked_query, alter_query):
        """ Initialize the baked query wrapper that will apply `alter_query` at the last moment """
        self._baked_query = baked_query
        self._alter_query = alter_query
        self._done_once = False

    # This is the method that's called in the end.
    # It will inject our alterations, and wrap the original one.
    def __call__(self, session):
        # SqlAlchemy will call this method when the query building is over,
        # so this is the best spot to apply our modifications .
        # This method will be called many times in a loop, so we have to apply our modifications only once.

        # Dot it just once
        if not self._done_once:
            # Make sure it's not cached after this point
            self._baked_query.spoil()

            # Apply our custom query
            self._baked_query = self._baked_query.add_criteria(self._alter_query)

            # Don't do it again :)
            self._done_once = True

        # Execute the query
        return self._baked_query(session)

    # Proxy all other methods to the original BakedQuery transparently
    def __getattr__(self, attr):
        return getattr(self._baked_query, attr)


class UnBakedQuery(object):
    """ A BakedQuery that will not cache anything,
        but apply all callables right away, and alter the query in the end """

    __slots__ = ('_lambdas', '_alter_query')

    @classmethod
    def Bakery(cls, bakery, alter_query):
        """ A Bakery that will actually use wrapped BakedQuery objects """
        return lambda initial_fn, *cache_args: \
            cls(initial_fn, alter_query)

    def __init__(self, initial_fn, alter_query):
        self._lambdas = [initial_fn]  # the list of callables
        self._alter_query = alter_query

    # Implement some methods that I've seen in use in SelectInLoader
    # Mimic the behavior of the original object closely enough.

    def add_criteria(self, fn, *cache_args):
        self._lambdas.append(fn)  # just collect them, ignoring cache

    def spoil(self):
        pass  # always spoiled :)

    # For this method, reuse the same logic from the original query
    def _add_lazyload_options(self, options, effective_path, cache_path=None):
        return BakedQuery._add_lazyload_options(self, options, effective_path, cache_path)

    def __call__(self, ssn):
        # Call the whole chain
        # Do it every time. No cache. No mercy.
        q = ssn
        for fn in self._lambdas:
            q = fn(q)

        # Alter it
        q = self._alter_query(q)

        # Done
        return q

# endregion


# Register the loader option

@loader_option()
def selectinquery(loadopt, relationship, alter_query):
    """Indicate that the given attribute should be loaded using SELECT IN eager loading,
    with a custom `alter_query(q)` callable that returns a modified query.
    """
    # The loader option just declares which class to use
    loadopt = loadopt.set_relationship_strategy(relationship, {"lazy": "selectin_query"})

    # Loader options don't let us pass any other data to the class, but we need our custom query in.
    # The only way is to use the loader option itself.
    # create_row_processor() method will pluck it out.
    assert 'alter_query' not in loadopt.local_opts  # I'm not too sure that there won't be a clash. If there is, we'll have to use a unique key per relationship.
    loadopt.local_opts['alter_query'] = alter_query

    # Done
    return loadopt


@selectinquery._add_unbound_fn
def selectinquery(relationship, alter_query):
    return _UnboundLoad.selectinquery(_UnboundLoad(), relationship, alter_query)


# The exported loader option
selectinquery = selectinquery._unbound_fn
