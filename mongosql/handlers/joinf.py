"""
### Filtering Join Operation
The [Join Operation](#join-operation) has the following behavior:
when you requested the loading of a relation, and there were no items found, an empty value is returned
(a `null`, or an empty array).

```javascript
// This one will return all users
// (even those that have no articles)
$.get('/api/user?query=' + JSON.stringify({
    join: ["articles"]  // Regular Join: `join`
}))
```

This `joinf` Filtering Join operation does just the same thing that `join` does;
however, if there were no related items, the primary one is also removed.

```javascript
// This one will return *only those users that have articles*
// (users with no articles will be excluded)
$.get('/api/user?query=' + JSON.stringify({
    joinf: ["articles"]  // Filtering Join: `joinf`
}))
```

This feature is, quite honestly, weird, and is only available for backward-compatibility with a bug that existed
in some early MongoSQL versions. It has proven to be useful in some cases, so the bug has been given a name and a
place within the MongoSQL library :)

Note that `joinf`` does not support `skip` and `limit`
on nested entities because of the way it's implemented with Postgres.
"""

from .join import MongoJoin

class MongoFilteringJoin(MongoJoin):
    """ Joining relations: perform a real SQL JOIN to the related model, applying a filter to the
        whole result set (!)

        Note that this will distort the results of the original query:
        essentially, it will only return entities *having* at least one related entity with
        the given condition.

        This means that if you take an `Article`, make a 'joinf' to `Article.author`,
        and specify a filter with `age > 20`,
        you will get articles and their authors,
        but the articles *will be limited to only teenage authors*.
    """

    query_object_section_name = 'joinf'

    def _choose_relationship_loading_strategy(self, mjp):
        if mjp.has_nested_query:
            # Quite intentionally, we will use a regular JOIN here.
            # It will remove rows that 1) have no related rows, and 2) do not match our filter conditions.
            # This is what the user wants when they use 'joinf' handler.
            return self.RELSTRATEGY_JOINF
        else:
            return self.RELSTRATEGY_EAGERLOAD

    # merge() is not implemented for joinf, because the results wouldn't be compatible
    merge = NotImplemented
