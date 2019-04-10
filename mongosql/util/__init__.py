from .raiseload_col import raiseload_col
from .selectinquery import selectinquery
from .reusable import Reusable
from .mongoquery_settings_handler import MongoQuerySettingsHandler
from .marker import Marker


import sys
if sys.version_info >= (3, 5, 0):  # That's when `typing` module became available
    from .settings_dict import MongoQuerySettingsDict, StrictCrudHelperSettingsDict
else:
    # Python 2, 3.4
    MongoQuerySettingsDict = dict
    StrictCrudHelperSettingsDict = dict
