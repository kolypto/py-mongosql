[![Build Status](https://api.travis-ci.org/kolypto/py-mongosql.png?branch=master)](https://travis-ci.org/kolypto/py-mongosql)
[![Pythons](https://img.shields.io/badge/python-3.6%E2%80%933.8-blue.svg)](.travis.yml)


MongoSQL
========

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



Table of Contents
=================

* <a href="#querying">Querying</a>
    * <a href="#query-object-syntax">Query Object Syntax</a>
    * <a href="#operations">Operations</a>
        * <a href="#project-operation">Project Operation</a>
        * <a href="#sort-operation">Sort Operation</a>
        * <a href="#filter-operation">Filter Operation</a>
        * <a href="#join-operation">Join Operation</a>
        * <a href="#filtering-join-operation">Filtering Join Operation</a>
        * <a href="#aggregate-operation">Aggregate Operation</a>
        * <a href="#group-operation">Group Operation</a>
        * <a href="#slice-operation">Slice Operation</a>
        * <a href="#count-operation">Count Operation</a>
    * <a href="#json-column-support">JSON Column Support</a>
* <a href="#mongosql-programming-interface">MongoSQL Programming Interface</a>
    * <a href="#mongoquery">MongoQuery</a>
        * <a href="#creating-a-mongoquery">Creating a MongoQuery</a>
        * <a href="#reusable">Reusable</a>
        * <a href="#querying-mongoqueryquery">Querying: MongoQuery.query()</a>
        * <a href="#getting-results-mongoqueryend">Getting Results: MongoQuery.end()</a>
        * <a href="#getting-all-sorts-of-results">Getting All Sorts of Results</a>
    * <a href="#mongoquery-configuration">MongoQuery Configuration</a>
    * <a href="#mongoquery-api">MongoQuery API</a>
        * <a href="#mongoquerymodel-handler_settingsnone">MongoQuery(model, handler_settings=None)</a>
        * <a href="#mongoqueryfrom_queryquery---mongoquery">MongoQuery.from_query(query) -> MongoQuery</a>
        * <a href="#mongoquerywith_sessionssn---mongoquery">MongoQuery.with_session(ssn) -> MongoQuery</a>
        * <a href="#mongoqueryqueryquery_object---mongoquery">MongoQuery.query(**query_object) -> MongoQuery</a>
        * <a href="#mongoqueryend---query">MongoQuery.end() -> Query</a>
        * <a href="#mongoqueryend_count---countingquery">MongoQuery.end_count() -> CountingQuery</a>
        * <a href="#mongoqueryresult_contains_entities---bool">MongoQuery.result_contains_entities() -> bool</a>
        * <a href="#mongoqueryresult_is_scalar---bool">MongoQuery.result_is_scalar() -> bool</a>
        * <a href="#mongoqueryresult_is_tuples---bool">MongoQuery.result_is_tuples() -> bool</a>
        * <a href="#mongoqueryget_final_query_object---dict">MongoQuery.get_final_query_object() -> dict</a>
        * <a href="#mongoqueryensure_loadedcols---mongoquery">MongoQuery.ensure_loaded(*cols) -> MongoQuery</a>
        * <a href="#mongoqueryget_projection_tree---dict">MongoQuery.get_projection_tree() -> dict</a>
        * <a href="#mongoqueryget_full_projection_tree---dict">MongoQuery.get_full_projection_tree() -> dict</a>
        * <a href="#mongoquerypluck_instanceinstance---dict">MongoQuery.pluck_instance(instance) -> dict</a>
        * <a href="#handlers">Handlers</a>
* <a href="#crud-helpers">CRUD Helpers</a>
    * <a href="#crudhelpermodel-handler_settings">CrudHelper(model, **handler_settings)</a>
    * <a href="#strictcrudhelper">StrictCrudHelper</a>
    * <a href="#crudviewmixin">CrudViewMixin()</a>
    * <a href="#saves_relationsfield_names">@saves_relations(*field_names)</a>
* <a href="#other-useful-tools">Other Useful Tools</a>
    * <a href="#modelpropertybagsmodel">ModelPropertyBags(model)</a>
    * <a href="#combinedbagbags">CombinedBag(**bags)</a>
    * <a href="#countingqueryquery">CountingQuery(query)</a>"

Querying
========

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

Operations
----------

### Project Operation

Projection corresponds to the `SELECT` part of an SQL query.

In MongoDB terminology, *projection* is the process of selection a subset of fields from a document.

Your models have many fields, but you do not always need them all. Oftentimes, all you need is just a small number
of them. That's when you use this operation that *projects* some fields for you.

The `projéct` operation lets you list the fields that you want to have in the data you get from the API endpoint.
You do this by either listing the fields that you need (called *include mode*), or listing the fields that you
*do not* need (called *exclude mode*).

The resulting data query on the back-end will only fetch the fields that you've requested, potentially saving a lot
of bandwidth.

An example of a projection would look like this:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    // only include the following fields
    project: ['id', 'first_name', 'last_name'],
}))
```

#### Syntax

The Project operation supports the following syntaxes:

* Array syntax.

    Provide an array of field names to be included.
    All the rest will be excluded.

    Example:

    ```javascript
    { project: ['login', 'first_name'] }
    ```

* String syntax

    Give a list of field names, separated by whitespace.

    Example:

    ```javascript
    { project: 'login first_name' }
    ```

* Object syntax.

    Provide an object of field names mapped to either a `1` (include) or a `0` (exclude).

    Examples:

    ```javascript
    { project: { 'a': 1, 'b': 1 } } # Include specific fields. All other fields are excluded
    { project: { 'a': 0, 'b': 0 } }  # Exclude specific fields. All other fields are included
    ```

    Note that you can't intermix the two: you either use all `1`s to specify the fields you want included,
    or use all `0`s to specify the fields you want excluded.

    NOTE: One special case is a so-called *full projection*: when your projection object mentions every single property
    of a model, then you're allowed to set `1`s to some, and `0`s to others in the same object. Use wisely.

#### Fields Excluded by Default
Note that some fields that exist on the model may not be included *by default*: this is something that
back-end developers may have configured with `default_exclude` setting on the server.

You will not receive those fields unless you explicitly require them.
This may be appropriate for some field that contain a lot of data, or require some calculation.

To include those fields, you have to request them explicitly: just use their name
in the list of fields that you request.

#### Related Models
Normally, in order to load a related model (say, user's `user_profile`, or some other data related to this model),
you would use the [Join Operation](#join-operation).

However, for convenience, you can now also load related models by just giving their name in the projection,
as if it was a field. For example:

```javascript
{ project: {
    id: 1,
    name: 1,
    user_articles: 1  // the related model will be loaded
}}
```

This request will load the related `user_articles` for you.

Note that some relationships will be disabled for security reasons.
### Sort Operation

Sorting corresponds to the `ORDER BY` part of an SQL query.

The UI would normally require the records to be sorted by some field, or fields.

The sort operation lets the API user specify the sorting of the results,
which makes sense for API endpoints that return a list of items.

An example of a sort operation would look like this:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    // sort by age, descending;
    // then sort by first name, alphabetically
    sort: ['age-', 'first_name+'],
}))
```

#### Syntax

* Array syntax.

    List of column names, optionally suffixed by the sort direction: `-` for `DESC`, `+` for `ASC`.
    The default is `+`.

    Example:

    ```javascript
    { sort: [ 'a+', 'b-', 'c' ] }  // -> a ASC, b DESC, c DESC
    ```

* String syntax

    List of columns, with optional `+` / `-`, separated by whitespace.

    Example:

    ```javascript
    { sort: 'a+ b- c' }
    ```

Object syntax is not supported because it does not preserve the ordering of keys.
### Filter Operation
Filtering corresponds to the `WHERE` part of an SQL query.

MongoSQL-powered API endpoints would typically return the list of *all* items, and leave it up to
the API user to filter them the way they like.

Example of filtering:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    // only select grown-up females
    filter: {
        // all conditions are AND-ed together
        age: { $gte: 18, $lte: 25 },  // age 18..25
        sex: 'female',  // sex = "female"
    }
}))
```

#### Field Operators
The following [MongoDB query operators](https://docs.mongodb.com/manual/reference/operator/query/)
operators are supported:

Supports the following MongoDB operators:

* `{ a: 1 }` - equality check: `field = value`. This is a shortcut for the `$eq` operator.
* `{ a: { $eq: 1 } }` - equality check: `field = value` (alias).
* `{ a: { $lt: 1 } }`  - less than: `field < value`
* `{ a: { $lte: 1 } }` - less or equal than: `field <= value`
* `{ a: { $ne: 1 } }` - inequality check: `field != value`.
* `{ a: { $gte: 1 } }` - greater or equal than: `field >= value`
* `{ a: { $gt: 1 } }` - greater than: `field > value`
* `{ a: { $prefix: 1 } }` - prefix: `field LIKE "value%"`
* `{ a: { $in: [...] } }` - any of. Field is equal to any of the given array of values.
* `{ a: { $nin: [...] } }` - none of. Field is not equal to any of the given array of values.
* `{ a: { $exists: true } }` - value is not `null`.

Supports the following operators on an `ARRAY` field, for a scalar value:

* `{ arr: 1 }`  - containment check: field array contains the given value: `ANY(array) = value`.
* `{ arr: { $ne: 1 } }` - non-containment check: field array does not contain value: `ALL(array_col) != value`.
* `{ arr: { $size: 0 } }` - Has a length of N (zero, to check for an empty array)


Supports the following operators on an `ARRAY` field, for an array value:

* `{ arr: [...] }`  - equality check: two arrays are completely equal: `arr = value`.
* `{ arr: { $ne: [...] } }` - inequality check: two arrays are not equal: `arr != value`.
* `{ arr: { $in: [...] } }` - intersection check. Check that the two arrays have common elements.
* `{ arr: { $nin: [...] } }` - no intersection check. Check that the two arrays have no common elements.
* `{ arr: { $all: [...] } }` - Contains all values from the given array

#### Boolean Operators

In addition to comparing fields to a value, the following boolean operators are supported
that enable you to make complex queries:

* `{ $or: [ {..criteria..}, .. ] }`  - any is true
* `{ $and: [ {..criteria..}, .. ] }` - all are true
* `{ $nor: [ {..criteria..}, .. ] }` - none is true
* `{ $not: { ..criteria.. } }` - negation

Example usage:

```javascript
$.get('/api/books?query=' + JSON.stringify({
    // either of the two options are fine
    $or: [
        // First option: sci-fi by Gardner Dozois
        { genre: 'sci-fi', editor: 'Gardner Dozois' },
        // Second option: any documentary
        { genre: 'documentary' },
    ]
}))
```

#### Related columns
You can also filter the data by the *columns on a related model*.
This is achieved by using a dot after the relationship name:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    filter: {
        // Fields of the 'user' model
        first_name: 'John',
        last_name: 'Doe',
        // Field of a related 'address' model
        'address.zip': '100098',
    }
}))
```
### Join Operation
Joining corresponds to the `LEFT JOIN` part of an SQL query (although implemented as a separate query).

In the back-end database, the data is often kept in a *normalized form*:
items of different types are kept in different places.
This means that whenever you need a related item, you'll have to explicitly request it.

The Join operation lets you load those related items.

Please keep in mind that most relationships would be disabled on the back-end because of security concerns about
exposing sensitive data. Therefore, whenever a front-end developer needs to have a relationship loaded,
it has to be manually enabled on the back-end! Please feel free to ask.

Examples follow.

#### Syntax

* Array syntax.

    In its most simple form, all you need to do is just to provide the list of names of the relationships that you
    want to have loaded:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        join: ['user_profile', 'user_posts'],
    }))
    ```

* String syntax.

    List of relationships, separated by whitespace:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        join: 'user_profile user_posts',
    }))
    ```

* Object syntax.

    This syntax offers you great flexibility: with a nested Query Object, it is now posible to apply operations
    to related entities: select just a few fields (projection), sort it, filter it, even limit it!

    The nested Query Object supports projections, sorting, filtering, even joining further relations, and
    limiting the number of related entities that are loaded!

    In this object syntax, the object is an embedded Query Object. For instance:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        join: {
            // Load related 'posts'
            posts: {
                filter: { rating: { $gte: 4.0 } },  // Only load posts with raing > 4.0
                sort: ['date-'],  // newest first
                skip: 0,  // first page
                limit: 100,  // 100 per page
            },

            // Load another relationship
            'comments': null,  # No specific options, just load
            }
        }
    }))
    ```

    Note that `null` can be used to load a relationship without custom querying.
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
### Aggregate Operation
Aggregation corresponds to the `SELECT ...` part of an SQL query with aggregation functions.

Sometimes the API user wouldn't need the data itself, but rather some statistics on that data: the smallest value,
the largest value, the average value, the sum total of all values.

This is what aggregation does: lets the API user execute statistical queries on the data.
Its features are limited, but in the spirit of MongoSQL, will save some routine work for back-end developers.

Example:
```javascript
$.get('/api/user?query=' + JSON.stringify({
    // The youngest and the oldest
    min_age: { $min: 'age' },
    max_age: { $max: 'age' },

    // SUM(1) for every user produces the total number of users
    number_of_users: { $sum: 1 },

    // Count the number of youngsters: age < 18
    // This is a SUM() of a boolean expression, which gives 1 for every matching row.
    youngster_count: { $sum: { age: { $lt: 18 } } },
}))
```

Note that for security reasons, aggregation must be manually enabled for every field on the back-end.

#### Syntax
The syntax is an object that declares custom field names to be used for keeping results:

    aggregate: { computed-field-name: <expression> }

The *expression* can be:

* Column name: essentially, projecting a column into the result set so that you can have the original value

    Example:

    ```javascript
    aggregate: {
        age: 'age'
    }
    ```

    This is only useful when combined with the [Group Operation](#group-operation).
    It is disabled by default on the back-end.

* Aggregation functions:

    * `{ $min: operand }` - smallest value
    * `{ $max: operand }` - largest value
    * `{ $avg: operand }` - average value
    * `{ $sum: operand }` - sum of values

    The *operand* can be:

    * Column name: to apply the aggregation function to a column

        Example:

        ```javascript
        aggregate: {
            min_age: { $min: 'age' }
        }
        ```

    * Boolean expression: see [Filter Operation](#filter-operation).

        This is a very useful trick.
        Because the result of a boolean expression is `1` when it's true, you can take a `$sum` of them,
        and count the number of rows that match that condition.

        Example:

        ```javascript
        // Count the number of youngsters: age < 18
        // This is a SUM() of a boolean expression, which gives 1 for every matching row.
        aggregate: {
            youngster_count: { $sum: { age: { $lt: 18 } } },
        }
        ```

    * Integer value (only supported by `$sum` operator)

        Example:

        ```javascript
        // Gives the total number of rows
        aggregate: {
            total: { $sum: 1 }  // one for every row. Can be 2 or 3 if you like
        }
        ```

Note that aggregation often makes sense only when used together with the [Group Operation](#group-operation).
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
### Slice Operation
Slicing corresponds to the `LIMIT .. OFFSET ..` part of an SQL query.

The Slice operation consists of two optional parts:

* `limit` would limit the number of items returned by the API
* `skip` would shift the "window" a number of items

Together, these two elements implement pagination.

Example:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    limit: 100, // 100 items per page
    skip: 200,  // skip 200 items, meaning, we're on the third page
}))
```

Values: can be a number, or a `null`.
### Count Operation
Slicing corresponds to the `SELECT COUNT(*)` part of an SQL query.

Simply, return the number of items, without returning the items themselves. Just a number. That's it.

Example:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    count: 1,
}))
```

The `1` is the *on* switch. Replace it with `0` to stop counting.

NOTE: In MongoSQL 2.0, there is a way to get both the list of items, *and* their count *simultaneously*.
This would have way better performance than two separate queries.
Please have a look: [CountingQuery](#countingqueryquery) and [MongoQuery.end_count()](#mongoqueryend_count---countingquery).


JSON Column Support
-------------------

A `JSON` (or `JSONB`) field is a column that contains an embedded object,
which itself has fields too. You can access these fields using a dot.

Given a model fields:

```javascript
model.data = { rating: 5.5, list: [1, 2, 3], obj: {a: 1} }
```

You can reference JSON field's internals:

```javascript
'data.rating'
'data.list.0'
'data.obj.a'
'data.obj.z'  // gives NULL when a field does not exist
```

Operations that support it:

* [Sort](#sort-operation) and [Group](#group-operation) operations:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        sort: ['data.rating']  // JSON field sorting
    }))
    ```

* [Filter](#filter-operation) operation:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        filter: {
            'data.rating': { $gte: 5.5 },  // JSON field condition
        }
    }))
    ```

    or this is how you test that a property is missing:

    ```javascript
    { 'data.rating': null }  // Test for missing property
    ```

    *CAVEAT*: PostgreSQL is a bit capricious about data types, so MongoSql tries to guess it *using the operand you provide*.
    Hence, when filtering with a property known to contain a `float`-typed field, please provide a `float` value!.

* [Aggregate](#aggregate-operation):

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        aggregate: {
            avg_rating: { $avg: 'data.rating' }
        }
    }))
    ```



























MongoSQL Programming Interface
==============================

MongoQuery
----------
### Creating a MongoQuery
`MongoQuery` is the main tool that lets you execute JSON Query Objects against an SqlAlchemy-handled database.

There are two ways to use it:

1. Construct `MongoQuery` manually, giving it your model:

    ```python
    from mongosql import MongoQuery
    from .models import User  # Your model

    ssn = Session()

    # Create a MongoQuery, using an initial Query (possibly, with some initial filtering applied)
    mq = MongoQuery(User).from_query(ssn.query(User))
    ```

2. Use the convenience mixin for your Base:

    ```python
    from sqlalchemy.ext.declarative import declarative_base
    from mongosql import MongoSqlBase

    Base = declarative_base(cls=(MongoSqlBase,))

    class User(Base):
        #...
    ```

    Using this Base, your models will have a shortcut method which returns a `MongoQuery`:

    ```python
    User.mongoquery(session)
    User.mongoquery(query)
    ```

    With `mongoquery()`, you can construct a query from a session:

    ```python
    mq = User.mongoquery(session)
    ```

    .. or from an [sqlalchemy.orm.Query](https://docs.sqlalchemy.org/en/latest/orm/query.html),
    which allows you to apply some initial filtering:

    ```python
    mq = User.mongoquery(
        session.query(User).filter_by(active=True)  # Only query active users
    )
    ```

### Reusable
A `MongoQuery` object itself is not reusable: it can make just one query.

However, it makes sense to save some initialization and keep it ready for new requests.
For performance reasons, this has to be done manually with the `Reusable` wrapper:

```python
mq_factory = Reusable(User.mongoquery(session))
```

The wrapped object has all the methods of a `MongoQuery`, but will make a proper copy when used.
Think of it as a factory.

### Querying: `MongoQuery.query()`
Once a `MongoQuery` is prepared, you can give it a QueryObject:

```python
# QueryObject
query_object = {
  'filter': {
    'sex': 'f',
    'age': { '$gte': 18, '$lte': 25 },  # 18..25 years
  },
  'order': ['weight+'],  #  slims first
  'limit': 50,  # just enough :)
}

# MongoQuery
mq = User.mongoquery(ssn).query(**query_object)
```

### Getting Results: `MongoQuery.end()`
Because `MongoQuery` is just a wrapper around an SqlAlchemy's `Query`, you can get that `Query`
and get results out of it:

```python
# By calling the `MongoQuery.end()` method, you get an SqlAlchemy `Query`:
q = mq.end()  # SqlALchemy Query

# Execute the query and fetch results
girls = q.all()
```

### Getting All Sorts of Results
Let's remember that the Query generated by MongoQuery can return three sorts of results:

1. Entities. When the API user has requested an entity of a list of them.
3. Integer. When the API user has used `{count: 1}`.
2. Tuples. This is what you get when the API user has used the [Aggregate Operation](#aggregate-operation).

`MongoQuery` has three methods that help you detect what you get:

1. `MongoQuery.result_contains_entities()`
2. `MongoQuery.result_is_scalar()`
3. `MongoQuery.result_is_tuples()`

Here's how to use it:

```python
def get_result(mq: MongoQuery, query: Query):
    # Handle: Query Object has count
    if mq.result_is_scalar():
        return {'count': query.scalar()}

    # Handle: Query Object has group_by and yields tuples
    if mq.result_is_tuples():
        # zip() column names together with the values, and make it into a dict
        return {
            'results': [dict(zip(row.keys(), row))
                        for row in query]
        }

    # Regular result: entities
    return {
        'users': query.all()
    }
```

Most likely, you won't need to handle that at all: just use  [CRUD Helpers](#crud-helpers)
that implement most of this logic for you.

MongoQuery Configuration
------------------------

`MongoQuery` has plenty of settings that lets you configure the way queries are made,
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

The available settings are:


* `default_projection`: (for: project)
    The default projection to use when no input was provided.
    When an input value is given, `default_projection` is not used at all: it overrides the default
    completely. If you want to merge some default into every projection, use some of the following settings:
    `default_exclude`, `force_include`, `force_exclude`

    NOTE: If you want the API to return *all fields* by default, use `None`. If you want the API to
    return *no fields* by default, use an empty list `[]`.
    This is because `None` is seen as "no default", and MongoSQL uses its internal default of including
    all fields; but `[]` is seen as an instruction "to include no fields by default".

* `default_exclude`: (for: project)
    A list of attributes that are excluded from every projection.
    The only way to load these attributes would be to request them explicitly.
    Use this for properties that contain a lot of data, or require extra queries.

* `default_exclude_properties`: (for: project)
    When `True`, all `@property` and `@hybrid_property` attributes
    will be excluded by default (put into `default_exclude`).
    This is a convenivent shortcut.
    Use `default_include_properties` to overrule.

* `default_unexclude_properties`: (for: project)
    The list of `@property` and `@hybrid_property` attributes that won't be excluded:
    they will be treated like the rest of the columns.

* `bundled_project`: (for: project)
    The dict that declares columns that depend on other columns being loaded.
    When you have a property that depends on some columns, and the user wants it loaded, the setting
    got to have the name of the property mapped to the list of dependent columns.
    Example: {'full_name': ['first_name', 'last_name']}
    The additional columns would be loaded quietly, without being included into the projection.

* `force_include`: (for: project)
    A list of attributes that will always be loaded and included into the output.

* `force_exclude`: (for: project)
    A list of attributes that will always be unloaded and excluded from the output.
    No matter what you do, you can't access them.

* `ensure_loaded`: (for: project)
    A list of columns that will be loaded even when the user didn't request them.
    These columns will be loaded quietly, however, without being included into the projection.
    Use case: columns which your code requires. It would break without them, in case the user excludes them.
    You wouldn't want to force include them, but you'd like to include them 'quietly'.

* `raiseload_col`: (for: project)
    Granular `raiseload`: only raise when columns are lazy loaded

* `raiseload_rel`: (for: join)
    Granular `raiseload`: only raise when relations are lazy loaded

* `raiseload`: (for: project, join)
    Raise an exception when a column or a relationship that was not loaded
    is accessed by the application.
    This would result in an additional SQL query, which is very slow.

    This is a performance safeguard: when the API user does not want certain columns,
    they are not loaded. However, when the application tries to access them.
    When `raiseload_col=True`, you'll need to load all the columns & relationships manually
    (with `undefer()` and `joinedload()`), or by using `MongoQuery.ensure_loaded()`.

* `aggregate_columns`: (for: aggregate)
    List of column names for which aggregation is enabled.
    All columns for which aggregation is not explicitly enabled are disabled.

* `aggregate_labels`: (for: aggregate)
    Whether to enable labelling columns (aliases).
    This features is mostly useless,
    but exists here to complete compatilibility with MongoDB queries.

* `force_filter`: (for: filter)
    A dictionary with a filter that will be forced onto every request;
    or a Python `callable(model)` that returns a filtering condition for Query.filter().

* `scalar_operators`: (for: filter)
    A dict of additional operators for scalar columns.
    A better way to declare global operators would be to subclass MongoFilter
    and declare the additional operators inside the class.

* `array_operators`: (for: filter)
    A dict of additional operators for array columns.

* `allowed_relations`: (for: join)
    An explicit list of relationships that can be loaded by the user.
    All other relationships will raise a DisabledError when a 'join' is attempted.

* `banned_relations`: (for: join)
    An list of relationships that cannot be loaded by the user: DisabledError will be raised.

* `max_items`: (for: limit)
    The maximum number of items that can be loaded with this query.
    The user can never go any higher than that, and this value is forced onto every query.

* `legacy_fields`: (for: everything)
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

* `aggregate_enabled`: Enable/disable the `aggregate` handler

* `count_enabled`: Enable/disable the `count` handler

* `filter_enabled`: Enable/disable the `filter` handler

* `group_enabled`: Enable/disable the `group` handler

* `join_enabled`: Enable/disable the `join` handler

* `joinf_enabled`: Enable/disable the `joinf` handler

* `limit_enabled`: Enable/disable the `limit` handler

* `project_enabled`: Enable/disable the `project` handler

* `sort_enabled`: Enable/disable the `sort` handler

* `related`: Settings for queries on related models, based on the relationship name.

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

* `related_models`: When configuring every relationship seems to be too much, and you just want to define
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




More settings are available through the [CRUD helper](#crud-helpers) settings,
which is an extension of [MongoQuery Configuration](#mongoquery-configuration):


* `writable_properties`: Are `@property` model attributes writable?

    When `False`, and incoming JSON object will only be allowed to set/modify real
    columns. The only way to save a value for a `@property` would be to use the
    `@saves_relations` decorator and handle the value manually.

    When `True`, even `@property` and `@hybrid_property` objects will be writable.
    Note that validation, as with other fields, is up to you.
    In order to be completely writable, it also has to be in the `rw_fields` list.

* `ro_fields`: The list of read-only fields.

    These fields can only be modified in the code.
    Whenever any of those fields is submitted to the API endpoint, it's ignored,
    and even removed from the incoming entity dict.

* `rw_fields`: The list of writable fields.

    When you have too many `ro_fields`, it may be easier to provide a list of
    those that are writable; all the rest become read-only.

* `const_fields`: The list of constant fields.

    These fields can only be set when an object is created, but never changed
    when it is modified.

* `query_defaults`: Default values for every Query Object.

    This is the default Query Object that provides the defaults for every query.
    For instance, this may be the default `limit: 100`, or a default `project` operator.

* `**mongoquery_settings`: more settings for `MongoQuery` (as described above)



MongoQuery API
--------------

### `MongoQuery(model, handler_settings=None)`
MongoQuery is a wrapper around SqlAlchemy's `Query` that can safely execute JSON Query Objects

### `MongoQuery.from_query(query) -> MongoQuery`
Specify a custom sqlalchemy query to work with.

It can have, say, initial filtering already applied to it.
It no default query is provided, _from_query() will use the default.


Arguments:


* `query: Query`: Initial sqlalchemy query to work with (e.g. with initial filters pre-applied)




Returns `MongoQuery`





### `MongoQuery.with_session(ssn) -> MongoQuery`
Query with the given sqlalchemy Session


Arguments:


* `ssn: Session`: The SqlAlchemy `Session` to use for querying




Returns `MongoQuery`





### `MongoQuery.query(**query_object) -> MongoQuery`
Build a MongoSql query from an object


Arguments:


* `**query_object`: The Query Object to execute.




Returns `MongoQuery`



Exceptions:


* `InvalidRelationError`: Invalid relationship name provided in the input

* `InvalidColumnError`: Invalid column name provided in the input

* `InvalidQueryError`: syntax error for any of the Query Object sections

* `InvalidQueryError`: unknown Query Object operations provided (extra keys)





### `MongoQuery.end() -> Query`
Get the resulting sqlalchemy `Query` object




Returns `Query`





### `MongoQuery.end_count() -> CountingQuery`
Get the result, and also count the total number of rows.

Be aware that the cost will be substantially higher than without the total number,
but still cheaper than two separate queries.

Numbers: this gives about 50% boost to small result sets, and about 15% boost to larger result sets.

See [CountingQuery](#countingqueryquery) for more details.




Returns `CountingQuery`





Example:

```python
q = User.mongoquery(ssn).query(...).end_count()

# Get the count
q.count  # -> 127

# Get results
list(q)  # -> [User, ...]

# (!) only one actual SQL query was made
```


### `MongoQuery.result_contains_entities() -> bool`
Test whether the result will contain entities.

This is normally the case in the absence of 'aggregate', 'group', and 'count' queries.




Returns `bool`





### `MongoQuery.result_is_scalar() -> bool`
Test whether the result is a scalar value, like with count

In this case, you'll fetch it like this:

    MongoQuery(...).end().scalar()




Returns `bool`





### `MongoQuery.result_is_tuples() -> bool`
Test whether the result is a list of keyed tuples, like with group_by

In this case, you might fetch it like this:

    res = MongoQuery(...).end()
    return [dict(zip(row.keys(), row)) for row in res], None




Returns `bool`





### `MongoQuery.ensure_loaded(*cols) -> MongoQuery`
Ensure the given columns, relationships, and related columns are loaded

Despite any projections and joins the user may be doing, make sure that the given `cols` are loaded.
This will ensure that every column is loaded, every relationship is joined, and none of those is included
into `projection` and `pluck_instance`.

This method is to be used by the application code to handle the following situation:
* The API user has requested only fields 'a', 'b', 'c' to be loaded
* The application code needs field 'd' for its operation
* The user does not want to see no 'd' in the output.
Solution: use ensure_loaded('d'), and then pluck_instance()

Limitations:

1. If the user has requested filtering on a relationship, you can't use ensure_loaded() on it.
    This method will raise an InvalidQueryError().
    This makes sense, because if your application code relies on the presence of a certain relationship,
    it certainly needs it fully loaded, and unfiltered.
2. If the request contains no entities (e.g. 'group' or 'aggregate' handlers are used),
   this method would throw an AssertionError

If all you need is just to know whether something is loaded or not, use MongoQuery.__contains__() instead.

Remember that every time you use ensure_loaded() on a relationship, you disable the possibility of filtering for it!


Arguments:


* `*cols`: Column names ('age'), Relation names ('articles'), or Related column names ('articles.name')




Returns `MongoQuery`



Exceptions:


* `ValueError`: invalid column or relationship name given.
        It does not throw `InvalidColumnError` because that's likely your error, not an error of the API user :)

* `InvalidQueryError`: cannot merge because the relationship has a filter on it





### `MongoQuery.get_final_query_object() -> dict`
Get the final Query Object dict (after all handlers have applied their defaults).

This Query Object will contain the name of every single handler, including those that were not given any input.




Returns `dict`





### `MongoQuery.get_projection_tree() -> dict`
Get a projection-like dict that maps every included column to 1,
and every relationship to a nested projection dict.




Returns `dict`: the projection





Example:

```python
MongoQuery(User).query(join={'articles': dict(project=('id',))}).handler_join.projection
#-> {'articles': {'id': 1}}
```

This is mainly useful for debugging nested Query Objects.


### `MongoQuery.get_full_projection_tree() -> dict`
Get a full projection tree that mentions every column, but only those relationships that are loaded




Returns `dict`





### `MongoQuery.pluck_instance(instance) -> dict`
Pluck an sqlalchemy instance and make it into a dict

This method should be used to prepare an object for JSON encoding.
This makes sure that only the properties explicitly requested by the user get included
into the result, and *not* the properties that your code may have loaded.

Projection and Join properties are considered.


Arguments:


* `instance: object`: object




Returns `dict`






### Handlers
In addition to this, `MongoQuery` lets you inspect the internals of the MongoQuery.
Every handler is available as a property of the `MongoQuery`:

* `MongoQuery.handler_project`: [handlers.MongoProject](mongosql/handlers/project.py)
* `MongoQuery.handler_sort`: [handlers.MongoSort](mongosql/handlers/sort.py)
* `MongoQuery.handler_group`: [handlers.MongoGroup](mongosql/handlers/group.py)
* `MongoQuery.handler_join`: [handlers.MongoJoin](mongosql/handlers/join.py)
* `MongoQuery.handler_joinf`: [handlers.MongoFilteringJoin](mongosql/handlers/joinf.py)
* `MongoQuery.handler_filter`: [handlers.MongoFilter](mongosql/handlers/filter.py)
* `MongoQuery.handler_aggregate`: [handlers.MongoAggregate](mongosql/handlers/aggregate.py)
* `MongoQuery.handler_limit`: [handlers.MongoLimit](mongosql/handlers/limit.py)
* `MongoQuery.handler_count`: [handlers.MongoCount](mongosql/handlers/count.py)

Some of them have methods which may be useful for the application you're building,
especially if you need to get some information out of `MongoQuery`.





CRUD Helpers
============

MongoSql is designed to help with data selection for the APIs.
To ease the pain of implementing CRUD for all of your models,
MongoSQL comes with a CRUD helper that exposes MongoSQL capabilities for querying to the API user.
Together with [RestfulView](https://github.com/kolypto/py-flask-jsontools#restfulview)
from [flask-jsontools](https://github.com/kolypto/py-flask-jsontools),
CRUD controllers are extremely easy to build.

## `CrudHelper(model, writable_properties=True, **handler_settings)`
Crud helper: an object that helps implement CRUD operations for an API endpoint:

* Create: construct SqlAlchemy instances from the submitted entity dict
* Read: use MongoQuery for querying
* Update: update SqlAlchemy instances from the submitted entity using a dict
* Delete: use MongoQuery for deletion

Source: [mongosql/crud/crudhelper.py](mongosql/crud/crudhelper.py)

This object is supposed to be initialized only once;
don't do it for every query, keep it at the class level!

Most likely, you'll want to keep it at the class level of your view:

```python
from .models import User
from mongosql import CrudHelper

class UserView:
    crudhelper = CrudHelper(
        # The model to work with
        User,
        # Settings for MongoQuery
        **MongoQuerySettingsDict(
            allowed_relations=('user_profile',),
        )
    )
    # ...
```

Note that during "create" and "update" operations, this class lets you write values
to column attributes, and also to @property that are writable (have a setter).
If this behavior (with writable properties) is undesirable,
set `writable_properties=False`

The following methods are available:

### `CrudHelper.query_model(query_obj=None, from_query=None) -> MongoQuery`
Make a MongoQuery using the provided Query Object

Note that you have to provide the MongoQuery yourself.
This is because it has to be properly configured with handler_settings.


Arguments:


* `query_obj: Union[Mapping, NoneType] = None`: The Query Object to use

* `from_query: Union[sqlalchemy.orm.query.Query, NoneType] = None`: An optional Query to initialize MongoQuery with




Returns `MongoQuery`



Exceptions:


* `exc.DisabledError`: A feature is disabled; likely, due to a configuration issue. See handler_settings.

* `exc.InvalidQueryError`: There is an error in the Query Object that the user has made

* `exc.InvalidRelationError`: Invalid relationship name specified in the Query Object by the user

* `exc.InvalidColumnError`: Invalid column name specified in the Query Object by the user





### `CrudHelper.create_model(entity_dict) -> object`
Create an instance from entity dict.

This method lets you set the value of columns and writable properties,
but not relations. Use @saves_relations to handle additional fields.


Arguments:


* `entity_dict: Mapping`: Entity dict




Returns `object`: Created instance



Exceptions:


* `InvalidColumnError`: invalid column

* `InvalidQueryError`: validation errors





### `CrudHelper.update_model(entity_dict, instance) -> object`
Update an instance from an entity dict by merging the fields

- Attributes are copied over
- JSON dicts are shallowly merged

Note that because properties are *copied over*,
this operation does not replace the entity; it merely updates the entity.

In other words, this method does a *partial update*:
only updates the fields that were provided by the client, leaving all the rest intact.


Arguments:


* `entity_dict: Mapping`: Entity dict

* `instance: object`: The instance to update




Returns `object`: New instance, updated



Exceptions:


* `InvalidColumnError`: invalid column

* `InvalidQueryError`: validation errors







## `StrictCrudHelper`
A Strict Crud Helper imposes defaults and limitations on the API user:

Source: [mongosql/crud/crudhelper.py](mongosql/crud/crudhelper.py)

- Read-only fields can not be set: not with create, nor with update
- Constant fields can be set initially, but never be updated
- Defaults for Query Object provide the default values for every query, unless overridden

The following behavior is implemented:

* By default, all fields are writable
* If ro_fields is provided, these fields become read-only, all other fields are writable
* If rw_fields is provided, ony these fields are writable, all other fields are read-only
* If const_fields, it is seen as a further limitation on rw_fields: those fields would be writable,
    but only once.

### `StrictCrudHelper(model, writable_properties=True, ro_fields=None, rw_fields=None, const_fields=None, query_defaults=None, **handler_settings)`
Initializes a strict CRUD helper

Note: use a `**StrictCrudHelperSettingsDict()` to help you with the argument names and their docs!


Arguments:


* `model: DeclarativeMeta`: The model to work with

* `writable_properties: bool = True`: 

* `ro_fields: Union[Iterable[str], Callable, NoneType] = None`: List of read-only property names, or a callable which gives the list

* `rw_fields: Union[Iterable[str], Callable, NoneType] = None`: List of writable property names, or a callable which gives the list

* `const_fields: Union[Iterable[str], Callable, NoneType] = None`: List of property names that are constant once set, or a callable which gives the list

* `query_defaults: Union[Iterable[str], Callable, NoneType] = None`: Defaults for every Query Object: Query Object will be merged into it.

* `**handler_settings`: Settings for the `MongoQuery` used to make queries








Example:

```python
from .models import User
from mongosql import StrictCrudHelper, StrictCrudHelperSettingsDict

class UserView:
    crudhelper = StrictCrudHelper(
        # The model to work with
        User,
        # Settings for MongoQuery and StrictCrudHelper
        **StrictCrudHelperSettingsDict(
            # Can never be set of modified
            ro_fields=('id',),
            # Can only be set once
            const_fields=('login',),
            # Relations that can be `join`ed
            allowed_relations=('user_profile',),
        )
    )
    # ...
```




## `CrudViewMixin()`
A mixin class for implementations of CRUD views.

This class is supposed to be re-initialized for every request.

To implement a CRUD view:
1. Implement some method to extract the Query Object from the request
2. Set `crudhelper` at the class level, initialize it with the proper settings
3. Implement the `_get_db_session()` and the `_get_query_object()` methods
4. If necessary, implement the `_save_hook()` to customize new & updated entities
5. Override `_method_list()` and `_method_get()` to customize its output
6. Override `_method_create()`, `_method_update()`, `_method_delete()` and implement saving to the DB
7. Use [`@saves_relations`](#saves_relationsfield_names) method decorator to handle custom fields in the input dict

For an example on how to use CrudViewMixin, see this implementation:
[tests/crud_view.py](tests/crud_view.py)

Attrs:
    _mongoquery (MongoQuery):
        The MongoQuery object used to process this query.

### `CrudViewMixin._get_db_session() -> Session`
(Abstract method) Get a DB session to be used for queries made in this view




Returns `Session`: sqlalchemy.orm.Session





### `CrudViewMixin._get_query_object() -> Mapping`
(Abstract method) Get the Query Object for the current query.

Note that the Query Object is not only supported for get() and list() methods, but also for
create(), update(), and delete(). This enables the API use to request a relationship right away.




Returns `Mapping`






### `CrudViewMixin._method_get(*filter, **filter_by) -> object`
(CRUD method) Fetch a single entity: as in READ, single entity

Normally, used when the user has supplied a primary key:

    GET /users/1


Arguments:


* `*filter`: Additional filter() criteria

* `**filter_by`: Additional filter_by() criteria




Returns `object`



Exceptions:


* `exc.InvalidQueryError`: Query Object errors made by the user

* `sqlalchemy.orm.exc.MultipleResultsFound`: Multiple found

* `sqlalchemy.orm.exc.NoResultFound`: Nothing found





### `CrudViewMixin._method_list(*filter, **filter_by) -> Iterable[object]`
(CRUD method) Fetch a list of entities: as in READ, list of entities

Normally, used when the user has supplied no primary key:

    GET /users/

NOTE: Be careful! This methods does not always return a list of entities!
It can actually return:
1. A scalar value: in case of a 'count' query
2. A list of dicts: in case of an 'aggregate' or a 'group' query
3. A list or entities: otherwise

Please use the following MongoQuery methods to tell what's going on:
MongoQuery.result_contains_entities(), MongoQuery.result_is_scalar(), MongoQuery.result_is_tuples()

Or, else, override the following sub-methods:
_method_list_result__entities(), _method_list_result__groups(), _method_list_result__count()


Arguments:


* `*filter`: Additional filter() criteria

* `**filter_by`: Additional filter_by() criteria




Returns `Iterable[object]`



Exceptions:


* `exc.InvalidQueryError`: Query Object errors made by the user





### `CrudViewMixin._method_create(entity_dict) -> object`
(CRUD method) Create a new entity: as in CREATE

Normally, used when the user has supplied no primary key:

    POST /users/
    {'name': 'Hakon'}


Arguments:


* `entity_dict: dict`: Entity dict




Returns `object`: The created instance (to be saved)



Exceptions:


* `exc.InvalidQueryError`: Query Object errors made by the user





### `CrudViewMixin._method_update(entity_dict, *filter, **filter_by) -> object`
(CRUD method) Update an existing entity by merging the fields: as in UPDATE

Normally, used when the user has supplied a primary key:

    POST /users/1
    {'id': 1, 'name': 'Hakon'}


Arguments:


* `entity_dict: dict`: Entity dict

* `*filter`: Criteria to find the previous entity

* `**filter_by`: Criteria to find the previous entity




Returns `object`: The updated instance (to be saved)



Exceptions:


* `exc.InvalidQueryError`: Query Object errors made by the user

* `sqlalchemy.orm.exc.MultipleResultsFound`: Multiple entities found with the filter condition

* `sqlalchemy.orm.exc.NoResultFound`: The entity not found





### `CrudViewMixin._method_delete(*filter, **filter_by) -> object`
(CRUD method) Delete an existing entity: as in DELETE

Normally, used when the user has supplied a primary key:

    DELETE /users/1

Note that it will load the entity from the database prior to deletion.


Arguments:


* `*filter`: Criteria to find the previous entity

* `**filter_by`: Criteria to find the previous entity




Returns `object`: The instance to be deleted



Exceptions:


* `exc.InvalidQueryError`: Query Object errors made by the user

* `sqlalchemy.orm.exc.MultipleResultsFound`: Multiple entities found with the filter condition

* `sqlalchemy.orm.exc.NoResultFound`: The entity not found






### `CrudViewMixin._mongoquery_hook(mongoquery) -> MongoQuery`
(Hook) A hook invoked in _mquery() to modify MongoQuery, if necessary

This is the last chance to modify a MongoQuery.
Right after this hook, it end()s, and generates an sqlalchemy Query.

Use self._current_crud_method to tell what is going on: create, read, update, delete?


Arguments:


* `mongoquery: MongoQuery`: 




Returns `MongoQuery`





### `CrudViewMixin._save_hook(new, prev=None)`
(Hook) Hooks into create(), update() methods, before an entity is saved.

This allows to make some changes to the instance before it's actually saved.
The hook is provided with both the old and the new versions of the instance (!).

Note that it is executed before flush(), so DB defaults are not available yet.


Arguments:


* `new: object`: The new instance

* `prev: object = None`: Previously persisted version (is provided only when updating).









### `CrudViewMixin._method_create_or_update_many(entity_dicts, *filter, **filter_by) -> Iterable[mongosql.util.bulk.EntityDictWrapper]`
(CRUD method) Create-or-update many objects (aka upsert): create if no PK, update with PK

This smart method can be used to save (upsert: insert & update) many objects at once.

It will *load* those objects that have primary key fields set and update them with _method_update().
It will *create* objects that do not have primary key fields with _method_create()
It will *delegate* to _method_create_or_update_many__create_arbitrary_pk() that have primary key fields
but were not found in the database.

Note that the method uses EntityDictWrapper to preserve the order of entity dicts
and return results associated with them:

* EntityDictWrapper.instance is the resulting instance to be saved
* EntityDictWrapper.error is the exception (if any). It's not raised! Raise it if you will.

Note that you may wrap entity dicts with EntityDictWrapper yourself.
In this case, you may:

* set EntityDictWrapper.skip = True to cause the method to ignore it completely


Arguments:


* `entity_dicts: Iterable[dict]`: 

* `*filter`: 

* `**filter_by`: 




Returns `Iterable[mongosql.util.bulk.EntityDictWrapper]`







## `@saves_relations(*field_names)`
A decorator that marks a method that handles saving some related models (or any other custom values)

Whenever a relationship is marked for saving with the help of this decorator,
it is plucked out of the incoming JSON dict, and after an entity is created,
it is passed to the method that this decorator decorates.

In addition to saving relationships, a decorated mthod can be used to save any custom properties:
they're plucked out of the incoming entity dict, and handled manually anyway.
Note that all attributes that do not exist on the model are plucked out, and the only way to handle them
is through this method.

NOTE: this method is executed before _save_hook() is.

Example usage:

```python
from mongosql import saves_relations
from mongosql import ABSENT  # unique marker used to detect values not provided

class UserView(CrudViewMixin):
    @saves_relations('articles')
    def save_articles(self, new: object, prev: object = None, articles = ABSENT):
        if articles is not ABSENT:
            ...  # articles-saving logic
```

NOTE: the handler method is called with two positional arguments, and the rest being keyword arguments:

    save_articles(new_instance, prev_instance, **relations_to_be_saved)

NOTE: If the user did not submit any related entity, the method is still called, with relationship argument = None.

Multiple relations can be provided: in this case, all of them are handled with one method.





Other Useful Tools
==================

## `ModelPropertyBags(model)`
Model Property Bags is the class that lets you get information about the model's columns.

This is the class that binds them all together: Columns, Relationships, PKs, etc.
All the meta-information about a certain Model is stored here:

- Columns
- Relationships
- Primary keys
- Nullable columns
- Properties and Hybrid Properties
- Columns of related models
- Writable properties

Whenever it's too much to inspect several properties, use a `CombinedBag()` over them,
which lets you get a column from a number of bags.

## `CombinedBag(**bags)`
A bag that combines elements from multiple bags.

This one is used when something can handle both columns and relationships, or properties and
columns. Because this depends on what you're doing, this generalized implementation is used.

In order to initialize it, you give them the bags you need as a dict:

    cbag = CombinedBag(
        col=bags.columns,
        rel=bags.related_columns,
    )

Now, when you get an item, you get the aliased name that you have used:

    bag_name, bag, col = cbag['id']
    bag_name  #-> 'col'
    bag  #-> bags.columns
    col  #-> User.id

This way, you can always tell which bag has the column come from, and handle it appropriately.

## `CountingQuery(query)`
`Query` object wrapper that can count the rows while returning results

This is achieved by SELECTing like this:

    SELECT *, count(*) OVER() AS full_count

In order to be transparent, this class eliminates all those tuples in results and still returns objects
like a normal query would. The total count is available through a property.


