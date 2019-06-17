"""

If you know how to query documents in MongoDB, you can query your database with the same language.
MongoSQL uses the familiar [MongoDB Query Operators](https://docs.mongodb.com/manual/reference/operator/query/)
language with a few custom additions.

The Query Object, in JSON format, will let you sort, filter, paginate, and do other things.
You would typically send this object in the URL query string, like this:

```
GET /api/user?query={"filter":{"age":{"$gte":18}}}
```

The name of the `query` argument, however, may differ from project to project.



Query Object Syntax
-------------------

A Query Object is a JSON object that the API user can submit to the server to change the way the results are generated.
It is an object with the following properties:

* `project`: [Project Operation](#project-operation) selects the fields to be loaded
* `sort`: [Sort Operation](#sort-operation) determines the sorting of the results
* `filter`: [Filter Operation](#filter-operation) filters the results, using your criteria
* `join`: [Join Operation](#join-operation) loads related models
* `joinf`: [Filtering Join Operation](#filtering-join-operation) loads related models with filtering
* `aggregate`: [Aggregate Operation](#aggregate-operation) lets you calculate statistics
* `group`: [Group Operation](#group-operation) determines how to group rows while doing aggregation
* `skip`, `limit`: [Rows slicing](#slice-operation): paginates the results
* `count`: [Counting rows](#count-operation) counts the number of rows without producing results

An example Query Object is:

```javascript
{
  project: ['id', 'name'],  # Only fetch these columns
  sort: ['age+'],  # Sort by age, ascending
  filter: {
    # Filter condition
    sex: 'female',  # Girls
    age: { $gte: 18 },  # Age >= 18
  },
  join: ['user_profile'],  # Load the 'user_profile' relationship
  limit: 100,  # Display 100 per page
  skip: 10,  # Skip first 10 rows
}
```

Detailed syntax for every operation is provided in the relevant sections.

Please keep in mind that while MongoSQL provides a query language that is rich enough for most typical tasks,
there would still be cases when an implementation of a custom API would be better, or even the only option available.

MongoSQL was not designed to be a complete replacement for the SQL; it was designed only to keep you from doing
repetitive work :) So it's absolutely fine that some queries that you may have in mind won't be possible with MongoSQL.
"""

from .project import MongoProject
from .sort import MongoSort
from .group import MongoGroup
from .join import MongoJoin, \
    MongoJoinParams
from .joinf import MongoFilteringJoin
from .filter import MongoFilter, \
    FilterExpressionBase, FilterBooleanExpression, FilterColumnExpression, FilterRelatedColumnExpression
from .aggregate import MongoAggregate, \
    AggregateExpressionBase, AggregateLabelledColumn, AggregateColumnOperator, AggregateBooleanCount
from .aggregate import MongoAggregateInsecure
from .limit import MongoLimit
from .count import MongoCount

# TODO: implement update operations on a model in MongoDB-style
# TODO: document MongoHandler classes
