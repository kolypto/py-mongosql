from typing import *
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql.elements import BinaryExpression
from .inspect import pluck_kwargs_from


class MongoQuerySettingsDict(dict):
    """ MongoQuery settings container.

        Is only used for nice autocompletion and documentation purposes only! :)

        However... it may allow custom tweaks for configurations, if you override it.
        Here are some ideas:

        * Default values (e.g. disable aggregation by default)
        * Related configuration defaults (e.g. disable joins by default)
        * Getting column names from columns automatically
        * Configuration merging (e.g. inherit configuration)
        * Automatic configuration of relationships (e.g. from other models)

        The keyword settings in this object are just plain kwargs names
        for every handler object's __init__ method,
        which are fed to subclasses of MongoQueryHandlerBase by MongoQuerySettingsHandler.

        In addition to that, there are '<handler-name>_enabled' settings,
        that can enable or disable a handler.

        A special key, `related`, lets you specify the settings for queries on related models.
        For example, a MongoQuery(Article) can specify settings for queries made with joins
        to a related User model:

            related={'author': { default_exclude=('password',) } }
    """

    def __init__(self,
                 # --- project
                 default_projection = None,
                 default_exclude = None,
                 default_exclude_properties = True,
                 default_unexclude_properties = None,
                 bundled_project = None,
                 force_include = None,
                 force_exclude = None,
                 ensure_loaded = None,
                 # --- project & join & joinf
                 raiseload_col = False,
                 raiseload_rel = False,
                 raiseload = False,
                 # --- aggregate
                 aggregate_columns = None,
                 aggregate_labels = False,
                 # --- filter
                 force_filter = None,
                 scalar_operators = None,
                 array_operators = None,
                 # --- join & joinf
                 allowed_relations = None,
                 banned_relations = None,
                 # --- limit
                 max_items = None,
                 # --- Misc
                 legacy_fields: Iterable[str] = None,
                 # --- enabled_handlers?
                 aggregate_enabled: bool = True,
                 count_enabled: bool = True,
                 filter_enabled: bool = True,
                 group_enabled: bool = True,
                 join_enabled: bool = True,
                 joinf_enabled: bool = True,
                 limit_enabled: bool = True,
                 project_enabled: bool = True,
                 sort_enabled: bool = True,
                 # --- Relations
                 related = None,
                 related_models = None,
                 ):
        """ `MongoQuery` has plenty of settings that lets you configure the way queries are made,
        to fine-tune their security limitations, and to implement some custom behaviors.

        These settings can be nicely kept in a [MongoQuerySettingsDict](mongosql/util/settings_dict.py)
        and given to MongoQuery as the second argument.

        Example:
            ```python
            from mongosql import MongoQuery, MongoQuerySettingsDict

            mq = MongoQuery(models.User, MongoQuerySettingsDict(
                bundled_project=dict(
                    # can only join to the following relations
                    allowed_relations=('articles', 'comments'),
                    # configure nested queries
                    related=dict(
                        manager=dict(
                            force_exclude=('password',),
                        )
                    ),
                    # enable aggregation for columns
                    aggregate_columns=('age',),
                ),
            ))
            ```
        
        Args:
            default_projection (dict[str, int] | list[str] | None): (for: project)
                The default projection to use when no input was provided.
                When an input value is given, `default_projection` is not used at all: it overrides the default
                completely. If you want to merge some default into every projection, use some of the following settings:
                `default_exclude`, `force_include`, `force_exclude`

                NOTE: If you want the API to return *all fields* by default, use `None`. If you want the API to
                return *no fields* by default, use an empty list `[]`.
                This is because `None` is seen as "no default", and MongoSQL uses its internal default of including
                all fields; but `[]` is seen as an instruction "to include no fields by default".
            default_exclude (list[str]): (for: project)
                A list of attributes that are excluded from every projection.
                The only way to load these attributes would be to request them explicitly.
                Use this for properties that contain a lot of data, or require extra queries.
            default_exclude_properties (bool): (for: project)
                When `True`, all `@property` and `@hybrid_property` attributes
                will be excluded by default (put into `default_exclude`).
                This is a convenivent shortcut.
                Use `default_include_properties` to overrule.
            default_unexclude_properties (list[str]): (for: project)
                The list of `@property` and `@hybrid_property` attributes that won't be excluded:
                they will be treated like the rest of the columns.
            bundled_project (dict[str, list]): (for: project)
                The dict that declares columns that depend on other columns being loaded.
                When you have a property that depends on some columns, and the user wants it loaded, the setting
                got to have the name of the property mapped to the list of dependent columns.
                Example: {'full_name': ['first_name', 'last_name']}
                The additional columns would be loaded quietly, without being included into the projection.
            force_include (list[str]): (for: project)
                A list of attributes that will always be loaded and included into the output.
            force_exclude (list[str]): (for: project)
                A list of attributes that will always be unloaded and excluded from the output.
                No matter what you do, you can't access them.
            ensure_loaded (list[str]): (for: project)
                A list of columns that will be loaded even when the user didn't request them.
                These columns will be loaded quietly, however, without being included into the projection.
                Use case: columns which your code requires. It would break without them, in case the user excludes them.
                You wouldn't want to force include them, but you'd like to include them 'quietly'.
            raiseload (bool): (for: project, join)
                Raise an exception when a column or a relationship that was not loaded
                is accessed by the application.
                This would result in an additional SQL query, which is very slow.

                This is a performance safeguard: when the API user does not want certain columns,
                they are not loaded. However, when the application tries to access them.
                When `raiseload_col=True`, you'll need to load all the columns & relationships manually
                (with `undefer()` and `joinedload()`), or by using `MongoQuery.ensure_loaded()`.
            raiseload_col (bool): (for: project)
                Granular `raiseload`: only raise when columns are lazy loaded
            raiseload_rel (bool): (for: join)
                Granular `raiseload`: only raise when relations are lazy loaded
            aggregate_columns (list[str]): (for: aggregate)
                List of column names for which aggregation is enabled.
                All columns for which aggregation is not explicitly enabled are disabled.
            aggregate_labels (bool): (for: aggregate)
                Whether to enable labelling columns (aliases).
                This features is mostly useless,
                but exists here to complete compatilibility with MongoDB queries.
            force_filter (dict | Callable): (for: filter)
                A dictionary with a filter that will be forced onto every request;
                or a Python `callable(model)` that returns a filtering condition for Query.filter().
            scalar_operators (dict[str, Callable]): (for: filter)
                A dict of additional operators for scalar columns.
                A better way to declare global operators would be to subclass MongoFilter
                and declare the additional operators inside the class.
            array_operators (dict[str, Callable]): (for: filter)
                A dict of additional operators for array columns.
            allowed_relations (list[str] | None): (for: join)
                An explicit list of relationships that can be loaded by the user.
                All other relationships will raise a DisabledError when a 'join' is attempted.
            banned_relations: (for: join)
                An list of relationships that cannot be loaded by the user: DisabledError will be raised.
            max_items: (for: limit)
                The maximum number of items that can be loaded with this query.
                The user can never go any higher than that, and this value is forced onto every query.
            legacy_fields (list[str] | None): (for: everything)
                The list of fields (columns, relationships) that used to exist, but do not anymore.
                These fields will be quietly ignored by all handlers. Note that they will still appear in projections
                from `project` and `join` handlers. If you rely on them, your code will have to be able to ignore
                those fields as well.

                This is implemented for introducing breaking changes into the code when developers might still refer
                to the old column which is simply not there anymore.

                When a relationship or a column has disappeared from the model, the recommended
                backwards-compatible approach is to have it both in `legacy_fields` and `force_include`,
                and a @property that provides some fake value for compatibility.
                This way, clients will always get something, even though they cannot join manually anymore.

            aggregate_enabled (bool): Enable/disable the `aggregate` handler
            count_enabled (bool): Enable/disable the `count` handler
            filter_enabled (bool): Enable/disable the `filter` handler
            group_enabled (bool): Enable/disable the `group` handler
            join_enabled (bool): Enable/disable the `join` handler
            joinf_enabled (bool): Enable/disable the `joinf` handler
            limit_enabled (bool): Enable/disable the `limit` handler
            project_enabled (bool): Enable/disable the `project` handler
            sort_enabled (bool): Enable/disable the `sort` handler

            related (dict | Callable | None):
                Settings for queries on related models, based on the relationship name.

                For example, when a `User` has a relationship named 'articles',
                you can put the 'articles' key into this setting, and configure
                how queries to the related models are made.

                This way, you can define a completely different set of settings when a model is
                queried through another model's relationship.

                ```python
                related = dict(
                    # handler_settings for nested queries may be configured per relationship
                    relation-name: dict,
                    relation-name: lambda: dict,
                    relation-name: None,  # will fall back to '*'
                    # The default
                    # If there's no default, or gives None, `related_models` will be used
                    '*': lambda relationship_name, target_model: dict | None,
                )
                # or
                related = lambda: dict
                ```

            related_models (dict | Callable | None):
                When configuring every relationship seems to be too much, and you just want to define
                common settings for every model, use this setting instead of 'related'.

                It will automatically configure every relationship based on the target model.

                ```python
                related_models = dict(
                    # handler_settings for nested queries may be configured per model
                    # note that you're supposed to use models, not their names!
                    Model: dict,
                    Model: lambda: dict,
                    Model: None,  # will fall back to '*'
                    # The default
                    # If there's no default, or it yields None, the default handler_settings is used
                    '*': lambda relationship_name, target_model: dict | None,
                    # Example:
                    '*': lambda *args: dict(join=False)  # disallow further joins
                )
                # or
                related_models = lambda: dict
                ```

                It can also be used as a default, when there's no custom configuration provided in
                the 'related' settings.

                The 'related_models' setting actually enables you to have one global dict that will
                define the "default" rules that apply to an entity, no matter how it's loaded:
                directly, or through a relationship of another model.

                ```python
                # Collect all your settings into one global dict
                all_settings = {
                    User: user_settings,
                    Article: article_settings,
                    Comment: comment_settings,
                }

                # and reference it recursively from every model:
                user_settings = dict(
                    related_models=lambda: all_settings
                )
                ```

                Be careful, though: if every model inherits its `allowed_relations`,
                it would be possible to get almost any object through a series of nested joins!
        """
        super(MongoQuerySettingsDict, self).__init__()
        self.update({k: v
                     for k, v in locals().items()
                     if k not in {'__class__', 'self'}})
        # NOTE: before you say your BOO at me for using locals(), consider the following...
        # we have 20+ variables we have to pass to the update() function, and we have to make sure we've forgotten none.
        # At the same time, this method is only called ONCE per model, during the initialization phase.
        # All of this tells me that this is just the right situation to summon locals() to our assitance.
        # Otherwise, we'll have a large, long, ugly list of variables, and we'll have to keep it updated every time
        # there is a new setting in town.
        # Therefore, locals().

    def and_more(self, **settings):
        """ Copy the object and add more settings to it """
        return self.__class__(**{**self, **settings})

    @classmethod
    def pluck_from(cls, dict, skip=('max_items',)):
        """ Initialize the class by plucking kwargs from a dictionary.

            This is useful when you have a dict with configuration for multiple classes, and you want to initialize
            this one by getting only the keys you need.

            Example: pluck MongoQuerySettingsDict from a StrictCrudHelperSettingsDict.

            Args:
                skip: List of key names to skip when copying. Sometimes it just does not make sense to copy all values.
        """
        kwargs = pluck_kwargs_from(dict,
                                   for_func=cls.__init__,
                                   skip=skip
                                   )
        return cls(**kwargs)


class StrictCrudHelperSettingsDict(MongoQuerySettingsDict):
    """ StrictCrudHelper + MongoQuery settings container. """
    def __init__(self,
                 writable_properties: bool = True,
                 ro_fields: Union[Tuple[str], Callable] = None,
                 rw_fields: Union[Tuple[str], Callable] = None,
                 const_fields: Union[Tuple[str], Callable] = None,
                 query_defaults: dict = None,

                 # The rest is MongoQuery settings
                 # StrictCrudHelper is able to put them apart
                 **mongoquery_settings
                 ):
        """ More settings are available through the [CRUD helper](#crud-helpers) settings,
        which is an extension of [MongoQuery Configuration](#mongoquery-configuration):

        Args:
            writable_properties (bool): Are `@property` model attributes writable?

                When `False`, and incoming JSON object will only be allowed to set/modify real
                columns. The only way to save a value for a `@property` would be to use the
                `@saves_relations` decorator and handle the value manually.

                When `True`, even `@property` and `@hybrid_property` objects will be writable.
                Note that validation, as with other fields, is up to you.
                In order to be completely writable, it also has to be in the `rw_fields` list.

            ro_fields (list[str]): The list of read-only fields.

                These fields can only be modified in the code.
                Whenever any of those fields is submitted to the API endpoint, it's ignored,
                and even removed from the incoming entity dict.

            rw_fields (list[str]): The list of writable fields.

                When you have too many `ro_fields`, it may be easier to provide a list of
                those that are writable; all the rest become read-only.

            const_fields (list[str]): The list of constant fields.

                These fields can only be set when an object is created, but never changed
                when it is modified.

            query_defaults (dict): Default values for every Query Object.

                This is the default Query Object that provides the defaults for every query.
                For instance, this may be the default `limit: 100`, or a default `project` operator.

            **mongoquery_settings: more settings for `MongoQuery` (as described above)
        """
        super(StrictCrudHelperSettingsDict, self).__init__(**mongoquery_settings)
        self.update({k: v  # See the parent method for an apology... :)
                     for k, v in locals().items()
                     if k not in {'__class__', 'self', 'mongoquery_settings'}})
