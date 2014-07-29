[![Build Status](https://api.travis-ci.org/kolypto/py-mongosql.png?branch=master)](https://travis-ci.org/kolypto/py-mongosql)


MongoSQL
========

SqlAlchemy queries with MongoDB-style.

Extremely handy if you want to expose limited querying capabilities with a JSON API while keeping it safe.

Table of Contents
=================


Querying
========

MongoSQL follows [MongoDB query operators](http://docs.mongodb.org/manual/reference/operator/query/) 
syntax with custom additions.

Source for syntax handlers: [mongosql/statements.py](mongosql/statements.py)

Operations
----------

### Projection Operation

Projection operation allows to specify which columns to include/exclude in the result set.

Produces the following queries through SqlAlchemy:
    
    SELECT a, b           FROM ...;
    SELECT      c, d, ... FROM ...;

* Dictionary syntax.
   
   Specify field names mapped to boolean values: `1` for inclusion, `0` for exclusion.

    ```python
    { 'a': 1, 'b': 1 }  # Include specific fields. All other fields are excluded
    { 'a': 0, 'b': 0 }  # Exclude specific fields. All other fields are included
    ```

* List syntax.

    List field names to include.
    
    ```python
    [ 'a', 'b' ]  # Include these fields only
    ```

* String syntax.
    
    List field names as a comma-separated string. 
    
    Optionally, prefix the string with `"+"` or `"-"` to switch between inclusion and exclusion modes:

    ```python
     'a,b,c'  # Include fields
    '+a,b,c'  # Include fields
    '-a,b,c'  # Exclude fields
    ```

### Sort Operation

Sort rows.

Produces the following queries through SqlAlchemy:
    
    SELECT ... FROM ... ORDER BY a ASC, b DESC, ...;

* Dictionary syntax.
    
    Map column names to sort direction: `-1` for `DESC`, `+1` for `ASC`:
    
    ```python
    from collections import OrderedDict
    OrderedDict({ 'a': +1, 'b': -1 })
    ```
    
* List syntax.
    
    List column names, optionally suffixed by the sort direction: `-` for `DESC`, `+` for `ASC`:
    
    ```python
    [ 'a+', 'b-', 'c' ]  # = { 'a': +1, 'b': -1, 'c': +1 }
    ```
    
* String syntax.

    Same as above, comma-separated:
    
    ```python
    'a+,b-,c'  # = { 'a': +1, 'b': -1, 'c': +1 }
    ```

### Group Operation

Group rows.

Produces the following queries through SqlAlchemy:
    
    SELECT ... FROM ... GROUP BY a, b DESC, ...;

Syntax: same as for [Sort Operation](#sort-operation).

### Filter Operation

Supports most of [MongoDB query operators](http://docs.mongodb.org/manual/reference/operator/query/), 
including array behavior (for PostgreSQL).

Supports the following MongoDB operators:

* `{ a: 1 }`  - equality check. For array: containment check.

    For scalar column: `col = value`.
    
    For array column: contains value: `ANY(array_col) = value`. 
    
    For array column and array value: array equality check: `array_col = value`.

* `{ a: { $lt: 1 } }`  - <
* `{ a: { $lte: 1 } }` - <=
* `{ a: { $ne: 1 } }` - inequality check. For array: not-containment check.
    
    For scalar column: `col != value`.
    
    For array column: does not contain value: `ALL(array_col) != value`. 
    
    For array column and array value: array inequality check: `array_col != value`.

* `{ a: { $gte: 1 } }` - >=
* `{ a: { $gt: 1 } }` - >
* `{ a: { $in: [...] } }` - any of. For arrays: intersection check.
    
    For scalar column: `col IN(values)`
    
    For array column: `col && ARRAY[values]`
    
* `{ a: { $nin: [...] } }` - none of. For arrays: empty intersection check.
    
    For scalar column: `col NOT IN(values)`
    
    For array column: `NOT( col && ARRAY[values] )`

* `{ a: { $exists: true } }` - `IS [NOT] NULL` check

* `{ arr: { $all: [...] } }` - For array columns: contains all values
* `{ arr: { $size: 0 } }` - For array columns: has a length of N

Supports the following boolean operators:

* `{ $or: [ {..criteria..}, .. ] }`  - any is true
* `{ $and: [ {..criteria..}, .. ] }` - all are true
* `{ $nor: [ {..criteria..}, .. ] }` - none is true
* `{ $not: { ..criteria.. } }` - negation

### Join Operation

Allows to eagerly load specific relations by name.

* List syntax.
    
    Relation names list.
    
    ```python
    [ 'posts', 'comments' ]
    ```

* String syntax
    
  Comma-separated relation names.
  
  ```python
  'posts, comments'
  ```

### Aggregate Operation

Allows to fetch aggregated values with the help of aggregation functions.

Dict syntax: custom name of the computed field mapped to an expression:
    
    { computed-field-name: expression }
   
The *<expression>* can be:

* Column name
* Aggregation operator:
    
    * `{ $min: operand }` -- smallest value
    * `{ $max: operand }` -- largest value
    * `{ $avg: operand }` -- average value
    * `{ $sum: operand }` -- sum of values

    The *<operand>* can be:
    
    * Column name
    * Boolean expression: see [Filter Operation](#filter-operation)
    * Integer value (only supported by `$sum` operator)
    
Examples:

```python
# Count people by age
# NOTE: should be used together with grouping by 'age'
{
  'age': 'age',  # Column value
  'n': { '$sum': 1 },  # Count
}  # -> SELECT age, count(*) AS n ...

# Average salary by profession
# NOTE: should be used together with grouping by 'profession'
{
  'prof': 'profession',
  'salary': { '$avg': 'salary' }
}  # -> SELECT profession AS prof, avg(salary) AS salary ...

# Count people matching certain conditions
{
  'adults':    { '$sum': { 'age': { '$gte': 18 } } },
  'expensive': { '$sum': { 'salary': { '$gt': 10000 } } }
}  # -> SELECT SUM(age >= 18) AS adults, SUM(salary > 10000) AS expensive ...
```

JSON Column Support
-------------------

PostgreSQL 9.3 supports [JSON column type](http://www.postgresql.org/docs/9.3/static/functions-json.html),
and so does MongoSQL! :)

To access sub-properties of a JSON field, use dot-notation.

Given a model field:
    
```python
model.data = { 'rating': 5.5, 'list': [1,2,3], 'obj': {'a': 1} }
```
    
You can reference JSON field properties:
    
```python
'data.rating'
'data.list.0'
'data.obj.a'
'data.obj.z'  # gives NULL
```
    
Operations that support it:

* [Sort](#sort-operation) and [Group](#group-operation) operations:
    
    ```python
    ['data.rating-']
    ```
    
* [Filter](#filter-operation) operation:
    
    ```python
    { 'data.rating': { '$gte': 5.5 } }
    { 'data.rating': None }  # Test for missing property
    ```
    
* [Aggregation](#aggregation):

    ```python
    { 'max_rating': { '$max': 'data.rating' } }
    ```

*NOTE*: PostgreSQL is a bit capricious about data types, so MongoSql tries to guess it using the operand you provide.
Hence, when filtering with a property known to contain a `float`-typed field, provide `float` values to it.
