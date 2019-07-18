from sqlalchemy.orm.strategy_options import loader_option, _UnboundLoad
from sqlalchemy.orm.strategies import SelectInLoader
from sqlalchemy.orm import properties
from sqlalchemy import log, util


@log.class_logger
@properties.RelationshipProperty.strategy_for(lazy="selectin_query")
class SelectInQueryLoader(SelectInLoader, util.MemoizedSlots):
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

    __slots__ = ('_alter_query', '_cache_key', '_bakery')

    def create_row_processor(self, context, path, loadopt, mapper, result, adapter, populators):
        # Pluck the custom callable that alters the query out of the `loadopt`
        self._alter_query = loadopt.local_opts['alter_query']
        self._cache_key = loadopt.local_opts['cache_key']

        # Call super
        return super(SelectInQueryLoader, self) \
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
    # See SmartInjectorBakedQuery, or AlteringBakedQuery, or UnBakedQuery

    def _memoized_attr__bakery(self):
        # Here we override the `self.bakery` attribute
        # We feed it with a callable that can fetch the information about the current query
        return SmartInjectorBakedQuery.bakery(
            lambda: (self._alter_query, self._cache_key),
            size=300  # we can expect a lot of different queries
        )


# region Bakery Wrapper that will apply alter_query() in the end

from sqlalchemy.ext.baked import Bakery, BakedQuery


class SmartInjectorBakedQuery(BakedQuery):
    """ A BakedQuery that is able to inject another callable at the very last step, and still use the cache

        The whole point of the trick is that the function that we want to hack into uses a series of
        q.add_criteria(), and ultimately, does q.__call__() in a loop.
        Our goal is to inject another q.add_criteria() right before the final __call__().

        To achieve that, we subclass BakedQuery, and do our injection in the overridden __call__(), once.
    """
    __slots__ = ('_alter_query', '_done_once', '_can_be_cached')

    @classmethod
    def bakery(cls, alter_query_getter, size=200, _size_alert=None):
        bakery = SmartInjectorBakery(cls, util.LRUCache(size, size_alert=_size_alert))  # Copied from sqlalchemy
        bakery.alter_query_getter(alter_query_getter)
        return bakery

    def __init__(self, bakery, initial_fn, args=(), alter_query=None, cache_key=None):
        """ Initialize the baked query wrapper that will apply `alter_query` at the last moment

            Here, we just pass everything down the chain,
            but add another item to `args`, which is our cache key
        """
        super(SmartInjectorBakedQuery, self).__init__(bakery, initial_fn, args + (cache_key,))
        self._alter_query = alter_query
        self._can_be_cached = cache_key is not None
        self._done_once = False

    def __call__(self, session):
        # This method will be called many times in a loop, so we have to inject only once.

        # Dot it just once
        if not self._done_once:
            # If no external cache key was provided, we can't cache
            if not self._can_be_cached:
                self.spoil()

            # Inject our custom query
            self.add_criteria(self._alter_query)
            self._done_once = True  # never again

        # Execute the query
        return super(SmartInjectorBakedQuery, self).__call__(session)


class SmartInjectorBakery(Bakery):
    """ A bakery that remembers its parent class and is able to load additional data from it.

        In our case, it remembers a getter function that asks the parent class to provice an
        `alter_query` function. It is then passed to a BakedQuery, and is injected at the very
        last stage.
    """
    __slots__ = ('_alter_query_getter', )

    def alter_query_getter(self, alter_query_getter):
        self._alter_query_getter = alter_query_getter

    def __call__(self, initial_fn, *args):
        # Copy-paste from Bakery.__call__()
        return self.cls(self.cache, initial_fn, args, *self._alter_query_getter())

# endregion


# Register the loader option

@loader_option()
def selectinquery(loadopt, relationship, alter_query, cache_key=None):
    """Indicate that the given attribute should be loaded using SELECT IN eager loading,
    with a custom `alter_query(q)` callable that returns a modified query.

    Args
    ----

    alter_query: Callable
        A callable(query) that alters the query produced by selectinloader
    cache_key: Hashable
        A value to use for caching the query (if possible)
    """
    # The loader option just declares which class to use
    loadopt = loadopt.set_relationship_strategy(relationship, {"lazy": "selectin_query"})

    # Loader options don't let us pass any other data to the class, but we need our custom query in.
    # The only way is to use the loader option itself.
    # create_row_processor() method will pluck it out.
    assert 'alter_query' not in loadopt.local_opts  # I'm not too sure that there won't be a clash. If there is, we'll have to use a unique key per relationship.
    loadopt.local_opts['alter_query'] = alter_query
    loadopt.local_opts['cache_key'] = cache_key

    # Done
    return loadopt


@selectinquery._add_unbound_fn
def selectinquery(relationship, alter_query, cache_key=None):
    return _UnboundLoad.selectinquery(_UnboundLoad(), relationship, alter_query, cache_key)


# The exported loader option
selectinquery = selectinquery._unbound_fn
