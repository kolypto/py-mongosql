from sqlalchemy.orm.path_registry import EntityRegistry
from sqlalchemy.orm.strategy_options import loader_option, _UnboundLoad, sa_exc
from sqlalchemy.orm.strategies import properties, LoaderStrategy


@loader_option()
def raiseload_col(loadopt, *attrs):
    # Note that if a PK is not undefer()ed, it will be raiseload_col()ed!
    # And that's alright, because this is how defer() works: no failsafe mechanism at all.

    # In case you want to undefer() PKs manually right here,
    # try to experiment with this thingie:
    # cloned.undefer(*(c.key for c in loadopt.path.entity.primary_key))
    # See the source code for load_only() for more info.

    return loadopt.set_column_strategy(
        attrs, {"raiseload_col": True}
    )


@raiseload_col._add_unbound_fn
def raiseload_col(*attrs):
    return _UnboundLoad().raiseload_col(*attrs)


@properties.ColumnProperty.strategy_for(raiseload_col=True)
class RaiseColumnLoader(LoaderStrategy):
    """ A property that raises an exception on attempted access """

    def create_row_processor(self, context, path, loadopt, mapper, result, adapter, populators):
        # Copied from sqlalchemy.orm.strategies.DeferredColumnLoader#create_row_processor,
        # altered to use self._load_for_state
        if not self.is_class_level:
            set_column_loader_for_local_state = self._raise_column_loader
            populators["new"].append((self.key, set_column_loader_for_local_state))
        else:
            populators["expire"].append((self.key, False))

    def _raise_column_loader(self, state, dict_, row):
        # See: sqlalchemy.orm.state.InstanceState#_instance_level_callable_processor
        # This property, `self.parent_property._deferred_column_loader`, actually contains a function
        # returned by that method: `_set_callable`, which initializes callables for an attribute.
        # A `callable` will be invoked when an action is required to get the value of an attribute,
        # e.g. deferred loading.
        # This is what we have to override.

        # First, call the original function to preserve SqlAlchemy behavior
        self.parent_property._deferred_column_loader(state, dict_, row)
        # Now, put our raiseloader there
        state.callables[self.key] = self._load_for_state

    def _raise(self):
        raise sa_exc.InvalidRequestError("'%s' is not available due to mongosql.raiseload_col" % (self,))

    def _load_for_state(self, *args, **kwargs):
        self._raise()


# raiseload_col() will raise an exception if column loading is attempted
raiseload_col = raiseload_col._unbound_fn
