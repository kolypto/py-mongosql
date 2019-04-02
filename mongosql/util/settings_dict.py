from typing import *
from sqlalchemy.orm import Query, Load
from sqlalchemy.ext.declarative import DeclarativeMeta


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
    """

    def __init__(self,
                 # TODO: docs for every key
                 # --- project
                 default_projection=None,
                 default_exclude=None,
                 force_include=None,
                 force_exclude=None,
                 # --- project & join & joinf
                 raiseload=False,
                 # --- aggregate
                 aggregate_columns: Iterable[str] = None,
                 aggregate_labels: bool = False,
                 # --- filter
                 force_filter: Union[dict, Callable[[Query, DeclarativeMeta, Load], Query]] = None,
                 scalar_operators: Mapping[str, Callable] = None,
                 array_operators: Mapping[str, Callable] = None,
                 # --- join & joinf
                 allowed_relations: Iterable[str] = None,
                 banned_relations: Iterable[str] = None,
                 # --- limit
                 max_items: int = None,
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
                 related: Union[Mapping, Callable] = None,
                 related_models: Union[Mapping, Callable] = None,
                 ):
        super(MongoQuerySettingsDict, self).__init__()
        self.update({k: v
                     for k, v in locals().items()
                     if k not in {'__class__', 'self'}})


class StrictCrudHelperSettingsDict(MongoQuerySettingsDict):
    """ StrictCrudHelper + MongoQuery settings container. """
    def __init__(self,
                 # The list of read-only fields
                 ro_fields: Union[Tuple[str], Callable] = None,
                 # The list of read-write fields; all the rest will be read-only
                 rw_fields: Union[Tuple[str], Callable] = None,
                 # Default values for every Query Object: Query Object will be merged into it.
                 query_defaults: dict = None,

                 # The rest is MongoQuery settings
                 # StrictCrudHelper is able to put them apart
                 **mongoquery_settings
                 ):
        super(StrictCrudHelperSettingsDict, self).__init__(**mongoquery_settings)
        self.update({k: v
                     for k, v in locals().items()
                     if k not in {'__class__', 'self', 'mongoquery_settings'}})
