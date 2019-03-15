import inspect

from ..exc import DisabledError


class QuerySettings(object):
    """ Settings keeper for MongoQuery

        This is essentially a helper which will feed the correct kwargs to every class.

        MongoSql handlers receive settings as kwargs to their __init__() methods,
        and those kwargs have unique names.

        This class will collect all settings as a single, flat array,
        and give each handler only the settings it wants.

        This approach will let us use a flat configuration dict.
        In addition, because some handlers have matching settings (e.g. join and joinf),
        both of those will receive them!
    """

    def __init__(self, settings):
        """ Store the settings for every handler

            :param settings: dict of handler kwargs
        """
        assert isinstance(settings, dict)

        #: Settings dict
        self._settings = settings  # we don't make a copy, because we don't modify it

        #: kwarg names for every handler
        self._handler_kwargs = {}
        #: kwarg default values
        self._kwarg_defaults = {}

        #: disabled handler names
        self._disabled_handlers = set()

        #: Nested MongoQuery settings (for related models)
        self._nested_settings = self._settings.get('related', {})

    def get_settings(self, handler_name, handler_cls):
        """ Get settings for the given handler

            Because we do not know in advance how many handlers we will have, what their names will be,
            and what classes implement them, we have to handle them one by one.

            Every time a class is given us, we analyze its __init__() method in order to know its kwargs and its default values.
            Then, we take the matching keys from the settings dict, we take defaults from the argument defaults,
            and make it all into `kwargs` that will be given to the class.

            In addition to that, if the settings contain `handler_name=False`, then it means it's disabled.
            is_handler_enabled() method will later tell that to MongoQuery.
        """
        # Now we know the handler name
        # See if it's actually disabled
        if not self._settings.get(handler_name, True):
            self._disabled_handlers.add(handler_name)

        # Analyze its __init__() method's kwargs
        argspec = inspect.getargspec(handler_cls.__init__)  # TODO: use signature() in Python 3.3

        # Get the names of the kwargs
        # We assume that every handler receives 2 positional arguments; the rest are kwargs
        n_args = len(argspec.args) - len(argspec.defaults or ())
        handler_kwargs_names = frozenset(argspec.args[n_args:])
        self._handler_kwargs[handler_name] = handler_kwargs_names

        # Get defaults for kwargs
        self._kwarg_defaults.update(
            # Put together argument names + default values
            zip(argspec.args[n_args:], argspec.defaults or ()))

        # Get the values for these kwargs
        handler_kwargs = {k: self._settings.get(k, self._kwarg_defaults[k])
                          for k in handler_kwargs_names}

        # Done
        return handler_kwargs

    def is_handler_enabled(self, handler_name):
        """ Test if the handler is enabled in the configuration """
        return handler_name not in self._disabled_handlers

    def raise_if_not_handler_enabled(self, model_name, handler_name):
        """ Raise an error if the handler is not enabled """
        if not self.is_handler_enabled(handler_name):
            raise DisabledError('Query handler "{}" is disabled for "{}"'
                                .format(handler_name, model_name))

    def raise_if_invalid_handler_settings(self):
        """ Check whether there were any typos in setting names

            After all handlers were initialized, we've had a chance to analyze all their keyword arguments.
            Now, we have the information about them, and we can check whether every kwarg was actually used.
            If not, there must be a typo.

            :raises: KeyError: Invalid settings provided
        """
        # Known keys
        handler_names = set(self._handler_kwargs.keys())
        valid_kwargs = set(self._kwarg_defaults.keys())
        other_known_keys = {'related'}

        # Merge all known keys into one
        all_known_keys = handler_names | valid_kwargs | other_known_keys

        # Provided keys
        provided_keys = set(self._settings.keys())

        # Result: unknown keys
        invalid_keys = provided_keys - all_known_keys

        # Raise?
        if invalid_keys:
            raise KeyError('Invalid settings were provided for MongoQuery: {}'
                           .format(','.join(invalid_keys)))

    def settings_for_nested_mongoquery(self, relation_name):
        # No explicit configuration: return an empty dict
        if relation_name not in self._nested_settings:
            return {}

        # Explicit configuration
        sets = self._nested_settings[relation_name]

        # It may be a dict, or a callable
        if callable(sets):
            # Call it, and store the result back into our dict
            sets = self._nested_settings[relation_name] = sets()

        # Done
        # Return a copy, because we don't want our copy to be modified
        return sets

    def __repr__(self):
        return repr('{}({})'.format(self.__class__.__name__, self._settings))
