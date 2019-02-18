from __future__ import absolute_import
from .sort import MongoSort


class MongoGroup(MongoSort):
    """ MongoDB-style grouping

        It has the same syntax as MongoSort, so we just reuse the code.

        See :cls:MongoSort
    """

    query_object_section_name = 'group'

    def __init__(self, model):
        super(MongoSort, self).__init__(model)

        # On input
        #: OderedDict() of a group spec: {key: +1|-1}
        self.group_spec = None

    def input(self, group_spec):
        super(MongoSort, self).input(group_spec)
        self.group_spec = self._input(group_spec)
        return self

    def compile_columns(self):
        return [
            self.supported_bags[name].desc() if d == -1 else self.supported_bags[name]
            for name, d in self.group_spec.items()
        ]
