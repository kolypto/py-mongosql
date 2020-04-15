"""
MongoSQL is a JSON query engine that lets you query [SqlAlchemy](http://www.sqlalchemy.org/)
like a MongoDB database.

The main use case is the interation with the UI:
every time the UI needs some *sorting*, *filtering*, *pagination*, or to load some
*related objects*, you won't have to write a single line of repetitive code!

It will let the API user send a JSON Query Object along with the REST request,
which will control the way the result set is generated:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    sort: ['first_name-'],  // sort by `first_name` DESC
    filter: { age: { $gte: 18 } },  // filter: age >= 18
    join: ['user_profile'],  // load related `user_profile`
    limit: 10,  // limit to 10 rows
}))
```

Tired of adding query parameters for pagination, filtering, sorting?
Here is the ultimate solution.

NOTE: currently, only tested with PostgreSQL.
"""

# SqlAlchemy versions
from sqlalchemy import __version__ as SA_VERSION
SA_12 = SA_VERSION.startswith('1.2')
SA_13 = SA_VERSION.startswith('1.3')

# Exceptions that are used here and there
from .exc import *

# MongoSQL needs a lot of information about the properties of your models.
# All this is handled by the following class:
from .bag import ModelPropertyBags, CombinedBag

# The heart of MongoSql are the handlers:
# that's where your JSON objects are converted to actual SqlAlchemy queries!
from . import handlers

# MongoQuery is the man that parses your QueryObject and applies the methods from MongoModel that
# implement individual fields.
from .query import MongoQuery

# SqlAlchemy declarative base that defines .mongomodel() and .mongoquery() on it
# That's just for your convenience.
from .sa import MongoSqlBase

# CrudHelper is something that enabled you to use JSON for:
# - Creation (i.e. save a record into DB using JSON)
# - Replacement (i.e. completely replace a record in the DB)
# - Modification (i.e. update some specific fields of a record)
# CrudHelper is something that you'll need when building JSON API that implements CRUD:
# Create/Read/Update/Delete
from .crud import CrudHelper, StrictCrudHelper, CrudViewMixin
from .crud import saves_relations, ABSENT

# Helpers
# Reusable query objects (so that you don't have to initialize them over and over again)
from mongosql.util import Reusable
# raiseload_col() that can be applied to columns, not only relationships
from mongosql.util import raiseload_col, raiseload_rel, raiseload_all
# selectinquery() relationship loader that supports custom queries
from mongosql.util import selectinquery
# `Query` object wrapper that is able to query and count() at the same time
from mongosql.util import CountingQuery
# Settings objects for MongoQuery and StrictCrudHelper
from mongosql.util import MongoQuerySettingsDict, StrictCrudHelperSettingsDict
