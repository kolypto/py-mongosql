from .raiseload_col import raiseload_col
from .selectinquery import selectinquery
from .reusable import Reusable
from .mongoquery_settings_handler import MongoQuerySettingsHandler
from .marker import Marker


import sys
if sys.version_info[0] == 3:
    from .settings_dict import MongoQuerySettingsDict, StrictCrudHelperSettingsDict
else:
    # Python 2
    # TODO: REMOVE ME IN PYTHON 3!
    MongoQuerySettingsDict = dict
    StrictCrudHelperSettingsDict = dict
