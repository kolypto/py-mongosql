"""
### Group Operation
Grouping corresponds to the `GROUP BY` part of an SQL query.

By default, the [Aggregate Operation](#aggregate-operation) gives statistical results over all rows.

For instance, if you've asked for `{ avg_age: { $avg: 'age' } }`, you'll get the average age of all users.

Oftentimes this is not enough, and you'll want statistics calculated over groups of items.
This is what the Group Operation does: specifies which field to use as the "group" indicator.

Better start with a few examples.

#### Example #1: calculate the number of users of every specific age.
We use the `age` field as the group discriminator, and the total number of users is therefore calculated per group.
The result would be: something like:

    age 18: 25 users
    age 19: 20 users
    age 21: 35 users
    ...

The code:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    // The statistics
    aggregate: {
        age: 'age',  // Get the unadulterated column value
        count: { $sum: 1 },  // The count
    },
    // The discriminator
    group: ['age'],  // we do not discriminate by sex this time... :)
}))
```

#### Example #2: calculate teh average salary per profession

```javascript
$.get('/api/user?query=' + JSON.stringify({
        prof: 'profession',
        salary: { '$avg': 'salary' }
    },
    group: ['profession_id'],
}))
```

#### Syntax
The Group Operator, as you have seen, receives an array of column names.

* Array syntax.

    List of column names, optionally suffixed by the sort direction: `-` for `DESC`, `+` for `ASC`.
    The default is `+`.

    Example:

    ```javascript
    { group: [ 'a+', 'b-', 'c' ] } // -> a ASC, b DESC, c DESC
    ```

* String syntax

    List of columns, with optional `+` / `-`, separated by whitespace.

    Example:

    ```javascript
    { group: 'a+ b- c' }
    ```

"""

from .sort import MongoSort


class MongoGroup(MongoSort):
    """ MongoDB-style grouping

        It has the same syntax as MongoSort, so we just reuse the code.

        See :cls:MongoSort
    """

    query_object_section_name = 'group'

    def __init__(self, model, bags, legacy_fields=None):
        # Legacy fields
        self.legacy_fields = frozenset(legacy_fields or ())

        # Parent
        super(MongoSort, self).__init__(model, bags)  # yes, call the base; not the parent

        # On input
        #: OderedDict() of a group spec: {key: +1|-1}
        self.group_spec = None

    def input(self, group_spec):
        super(MongoSort, self).input(group_spec)  # call base; not the parent
        self.group_spec = self._input(group_spec)
        return self

    def compile_columns(self):
        return [
            self.supported_bags.get(name).desc() if d == -1 else self.supported_bags.get(name)
            for name, d in self.group_spec.items()
        ]

    # Not Implemented for this Query Object handler
    compile_options = NotImplemented
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation=None):
        if not self.group_spec:
            return query  # short-circuit

        return query.group_by(*self.compile_columns())

    def get_final_input_value(self):
        return [f'{name}{"-" if d == -1 else ""}'
                for name, d in self.group_spec.items()]
