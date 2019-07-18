import json
from exdoc import doc, getmembers, subclasses

# Methods
doccls = lambda cls, *allowed_keys: {
    'cls': doc(cls),
    'attrs': {name: doc(m, cls)
              for name, m in getmembers(cls, None,
                                        lambda key, value: key in allowed_keys or not key.startswith('_'))}
}

docmodule = lambda mod: {
    'module': doc(mod),
    'members': [ doc(getattr(mod, name)) for name in mod.__all__]
}

# Data
import mongosql
from mongosql.handlers import project, sort, group, filter, join, joinf, aggregate, limit, count
from mongosql import query, MongoQuerySettingsDict, StrictCrudHelperSettingsDict
from mongosql import ModelPropertyBags, CombinedBag, CountingQuery
from mongosql.crud import crudhelper, CrudHelper, StrictCrudHelper, CrudViewMixin, saves_relations

data = dict(
    mongosql=doc(mongosql),
    handlers=doc(mongosql.handlers),
    operations={
        m.__name__.rsplit('.', 1)[1]: doc(m)
        for m in (project, sort, group, filter, join, joinf, aggregate, limit, count)},
    mongosql_query=doc(mongosql.query),

    MongoQuery=doccls(mongosql.query.MongoQuery),
    MongoQuerySettingsDict_init=doc(MongoQuerySettingsDict.__init__, MongoQuerySettingsDict),
    StrictCrudHelperSettingsDict_init=doc(StrictCrudHelperSettingsDict.__init__, StrictCrudHelperSettingsDict),

    crudhelper=doc(crudhelper),
    CrudHelper=doccls(CrudHelper),
    StrictCrudHelper=doccls(StrictCrudHelper),
    CrudViewMixin=doccls(CrudViewMixin, *dir(CrudViewMixin)),
    saves_relations=doccls(saves_relations),

    ModelPropertyBags=doccls(ModelPropertyBags),
    CombinedBag=doccls(CombinedBag),
    CountingQuery=doccls(CountingQuery),
)

# Patches

class MyJsonEncoder(json.JSONEncoder):
    def default(self, o):
        # Classes
        if isinstance(o, type):
            return o.__name__
        return super(MyJsonEncoder, self).default(o)

# Document
print(json.dumps(data, indent=2, cls=MyJsonEncoder))
