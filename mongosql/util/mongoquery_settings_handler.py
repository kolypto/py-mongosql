import inspect
from sqlalchemy.ext.declarative import DeclarativeMeta

from ..exc import DisabledError


class MongoQuerySettingsHandler(object):
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

        #: Nested MongoQuery settings (for relations)
        self._nested_relation_settings = call_if_callable(self._settings.get('related', None)) or {}

        #: Nested MongoQuery settings (for related models)
        self._nested_model_settings = call_if_callable(self._settings.get('related_models', None))or {}

    def validate_related_settings(self, bags):
        """ Validate the settings for related entities.

            This method only validates the keys for "related" and "related_models".

            :type bags: mongosql.bag.ModelPropertyBags
            :raises KeyError: Invalid keys
        """
        # Validate "related": all keys must be relationship names
        invalid_keys = set(self._nested_relation_settings.keys()) - bags.relations.names - {'*'}
        if invalid_keys:
            raise KeyError('Invalid relationship name provided to "related": {!r}'
                           .format(list(invalid_keys)))

        # Validated "related_models": all keys must be models, not names
        invalid_keys = set(v
                           for v in self._nested_model_settings.keys()
                           if not isinstance(v, DeclarativeMeta))
        invalid_keys -= {'*'}
        if invalid_keys:
            raise KeyError('Invalid related model object provided to "related_models": {!r}'
                           .format(list(invalid_keys)))

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
        if not self._settings.get('{}_enabled'.format(handler_name), True):
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
        handler_names = set('{}_enabled'.format(handler_name)
                            for handler_name in self._handler_kwargs.keys())
        valid_kwargs = set(self._kwarg_defaults.keys())
        other_known_keys = {'related', 'related_models'}

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

    def _get_settings(self, store, key, star_lambda_args):
        """ Get settings from `store`, which is "related" or "related_models"

        handler_settings may be stored in two dict keys:
        * `related` is keyed by relation_name
        * `related_models` is keyed by target_model
        * Both map the key either a dict, or a lambda: dict | None,
        * Both have the default catch-all '*'
        * Both keep looking when a `None` is discovered

        Because of these similarities, this method handles them both.

        :param store: `self._nested_relation_settings` or `self._nested_model_settings`
        :param key: `relation_name`, or `target_model`
        :param args: Arguments passed to '*' lambda-handler
        :return: dict | None
        """
        # Try to get it by key
        sets = store.get(key, None)

        # callable?
        if callable(sets):
            sets = sets() if key != '*' else sets(*star_lambda_args)

        # Found?
        if sets is not None:
            return sets

        # Fallback: '*'
        if key != '*':
            return self._get_settings(store, '*', star_lambda_args)
        else:
            # Not found
            return None

    def settings_for_nested_mongoquery(self, relation_name, target_model):
        """ Get settings for a nested MongoQuery

        Tries in turn:
        related[relation-name]
        related[*]
        related_models[target-model]
        related_models[*]

        :param relation_name:
        :param target_model:
        :return:
        """
        # Try "related"
        sets = self._get_settings(self._nested_relation_settings, relation_name, (relation_name, target_model))

        # Try "related_models"
        if sets is None:
            sets = self._get_settings(self._nested_model_settings, target_model, (relation_name, target_model))

        # Done
        return sets

    def __repr__(self):
        return repr('{}({})'.format(self.__class__.__name__, self._settings))



call_if_callable = lambda v: v() if callable(v) else v
