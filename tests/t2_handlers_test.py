import unittest
from collections import OrderedDict
from sqlalchemy.orm import Load

from mongosql import Reusable, MongoQuery
from mongosql.handlers import *
from mongosql.exc import InvalidColumnError, DisabledError, InvalidQueryError, InvalidRelationError
from .models import *
from .util import stmt2sql


class HandlersTest(unittest.TestCase):
    """ Test individual handlers """

    longMessage = True
    maxDiff = None

    def test_projection(self):
        def test_by_full_projection(p, **expected_full_projection):
            """ Test:
                * get_full_projection()
                * __contains__() of a projection using its full projection
                * compile_columns()
            """
            self.assertEqual(p.get_full_projection(), expected_full_projection)

            # Test properties: __contains__()
            for name, include in expected_full_projection.items():
                self.assertEqual(name in p, True if include else False)

            # Test: compile_columns() only returns column properties
            columns = p.compile_columns()
            self.assertEqual(
                set(col.key for col in columns),
                set(col_name
                    for col_name in p.bags.columns.names
                    if expected_full_projection.get(col_name, 0))
            )

        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            MongoProject(Article).input(None).input(None)

        # === Test: No input
        p = MongoProject(Article).input(None)
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict())

        # === Test: Valid projection, array
        p = MongoProject(Article).input(['id', 'uid', 'title'])
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, uid=1, title=1))

        test_by_full_projection(p,
                                # Explicitly included
                                id=1, uid=1, title=1,
                                # Implicitly excluded
                                theme=0, data=0,
                                # Properties excluded
                                calculated=0, hybrid=0,
                                )

        # === Test: Valid projection, dict, include mode
        p = MongoProject(Article).input(dict(id=1, uid=1, title=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, uid=1, title=1))

        test_by_full_projection(p, # basically, the same thing
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=0, hybrid=0,
                                )

        # === Test: Valid projection, dict, exclude mode
        p = MongoProject(Article).input(dict(theme=0, data=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0, data=0))

        test_by_full_projection(p,
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=1, hybrid=1,
                                )

        # === Test: `default_exclude` in exclude mode
        p = MongoProject(Article, default_exclude=('calculated', 'hybrid'))\
            .input(dict(theme=0, data=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0, data=0,
                                            # Extra stuff
                                            calculated=0, hybrid=0))

        test_by_full_projection(p,
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=0, hybrid=0,  # now excluded
                                )

        # === Test: `default_exclude` in include mode (no effect)
        p = MongoProject(Article, default_exclude=('calculated', 'hybrid')) \
            .input(dict(id=1, calculated=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, calculated=1))

        test_by_full_projection(p,
                                id=1, uid=0, title=0,
                                theme=0, data=0,
                                calculated=1, hybrid=0,  # one included, one excluded
                                )

        # === Test: default_projection
        pr = Reusable(MongoProject(Article, default_projection=dict(id=1, title=1)))

        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))

        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))

        # === Test: merge

        mk_pr = lambda mode: MongoProject(Article).input(dict.fromkeys(['id', 'uid', 'title'], mode))

        # Originally include, merge include
        pr = mk_pr(1).merge(dict(data=1))
        self.assertEqual(pr.mode, pr.MODE_INCLUDE)
        self.assertEqual(pr.projection, dict(id=1, uid=1, title=1,
                                             # One new appended
                                             data=1))

        # Originally exclude, merge exclude
        pr = mk_pr(0).merge(dict(data=0))
        self.assertEqual(pr.mode, pr.MODE_EXCLUDE)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0,
                                             # One new appended
                                             data=0))

        # Originally include, merge exclude (conflict, just drop banned keys)
        pr = mk_pr(1).merge(dict(uid=0))
        self.assertEqual(pr.mode, pr.MODE_INCLUDE)
        self.assertEqual(pr.projection, dict(id=1, title=1))

        # Originally exclude, merge include (conflict, results in full projection)
        pr = mk_pr(0).merge(dict(data=1))
        self.assertEqual(pr.mode, pr.MODE_MIXED)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0,
                                             # Full projection in mixed mode
                                             theme=1, data=1, calculated=1, hybrid=1))

        # === Test: merge, quiet mode
        # Originally include, merge include
        pr = mk_pr(1).merge(dict(data=1), quietly=True)
        self.assertEqual(pr.get_full_projection(),
                         dict(id=1, uid=1, title=1, theme=0, data=0, calculated=0, hybrid=0))  # not 'data'

        # Originally exclude, merge include (conflict, results in full projection)
        pr = mk_pr(0).merge(dict(title=1), quietly=True)
        self.assertEqual(pr.get_full_projection(),
                         dict(id=0, uid=0, title=0, theme=1, data=1, calculated=1, hybrid=1))  # not 'title'

        # === Test: force_include
        pr = Reusable(MongoProject(Article, force_include=('id',)))

        # Include mode
        p = pr.input(dict(title=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))  # id force included
        # Exclude mode
        p = pr.input(dict(data=0))
        self.assertEqual(p.mode, p.MODE_MIXED)
        self.assertEqual(p.projection, dict(id=1,  # force included
                                            uid=1, title=1, theme=1,
                                            data=0,  # excluded by request
                                            calculated=1, hybrid=1))

        # === Test: force_exclude
        pr = Reusable(MongoProject(Article, force_exclude=('data',)))
        # Include mode
        p = pr.input(dict(id=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1))  # no `data`
        # Include mode: same property
        p = pr.input(dict(id=1, data=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1))  # No more data, even though requested
        # Exclude mode
        p = pr.input(dict(theme=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0,  # excluded by request
                                            data=0,  # force excluded
                                            ))

        # === Test: Invalid projection, dict, problem: invalid arguments passed to __init__()
        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, default_projection=dict(id=1, INVALID=1))
        with self.assertRaises(InvalidQueryError):
            MongoProject(Article, default_projection=dict(id=1, title=0))

        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, default_exclude='id')
        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, default_exclude=('INVALID',))

        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, force_exclude='id')
        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, force_exclude=('INVALID',))

        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, force_include='id')
        with self.assertRaises(InvalidColumnError):
            MongoProject(Article, force_include=('INVALID',))

        # === Test: Invalid projection, dict, problem: 1s and 0s
        pr = Reusable(MongoProject(Article))

        with self.assertRaises(InvalidQueryError):
            pr.input(dict(id=1, title=0))

        # === Test: Invalid projection, dict, problem: invalid column
        with self.assertRaises(InvalidColumnError):
            pr.input(dict(INVALID=1))

        # === Test: A mixed object is only acceptable when it mentions EVERY column
        # No error
        MongoProject(Article).input(dict(id=1, uid=1, title=1, theme=1, data=0,
                                         calculated=1, hybrid=1))

        # === Test: pluck_instance()
        a = Article(id=100, uid=10, title='title', theme='theme', data=dict(a=1), user=User(age=21))
        pr = MongoProject(Article).input(dict(id=1, uid=1, calculated=1))

        d = pr.pluck_instance(a)
        self.assertEqual(d, dict(id=100, uid=10, calculated=15))

        # === Test: dry run of compile_*()
        # No errors
        for input_value in (None, ('id',), {'id': 1}, {'id': 0}):
            MongoProject(Article).input(input_value).compile_options(Load(Article))

    def test_sort(self):
        sr = Reusable(MongoSort(Article))

        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            MongoSort(Article).input(None).input(None)

        # === Test: no input
        s = sr.input(None)
        self.assertEqual(s.sort_spec, OrderedDict())

        # === Test: list
        s = sr.input(['id', 'uid+', 'title-'])
        self.assertEqual(s.sort_spec, OrderedDict([('id', +1),('uid', +1),('title', -1)]))

        # === Test: OrderedDict
        s = sr.input(OrderedDict([('id', +1),('uid', +1),('title', -1)]))
        self.assertEqual(s.sort_spec, OrderedDict([('id', +1),('uid', +1),('title', -1)]))

        # === Test: dict
        # One item allowed
        s = sr.input(dict(id=-1))
        # Two items disallowed
        with self.assertRaises(InvalidQueryError):
            s = sr.input(dict(id=-1, uid=+1))

        # === Test: invalid columns
        with self.assertRaises(InvalidColumnError):
            # Invalid column
            sr.input(dict(INVALID=+1))

        with self.assertRaises(InvalidColumnError):
            # Properties not supported
            sr.input(dict(calculated=+1))

        # Hybrid properties are ok
        sr.input(dict(hybrid=+1))

        # === Test: JSON column fields
        sr.input({'data.rating': -1})

        # === Test: dry run of compile_*()
        # No errors
        for input_value in (None, ('id',), {'id': +1}):
            MongoSort(Article).input(input_value).compile_columns()

    def test_group(self):
        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            MongoGroup(Article).input(None).input(None)

        # === Test: list
        g = MongoGroup(Article).input(['uid'])
        self.assertEqual(g.group_spec, OrderedDict(uid=+1))

        g = MongoGroup(Article).input(['uid-'])
        self.assertEqual(g.group_spec, OrderedDict(uid=-1))

    def test_filter(self):
        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            MongoFilter(Article).input(None).input(None)

        # === Test: empty
        f = MongoFilter(Article).input(None)  # no problem

        # === Test: simple key=value object
        f = MongoFilter(Article).input(OrderedDict([
            ('id', 1),
            ('hybrid', True),  # No error
            ('data.rating', 10),  # Accessing JSON column
        ]))
        self.assertEqual(len(f.expressions), 3)

        e = f.expressions[0]  # type e: FilterColumnExpression
        self.assertIsInstance(e, FilterColumnExpression)
        self.assertEqual(e.column_name, 'id')
        self.assertEqual(e.column.key, 'id')
        self.assertEqual(e.operator_str, '$eq')  # inserted
        self.assertEqual(e.value, 1)
        self.assertEqual(stmt2sql(e.compile_expression()), 'a.id = 1')

        e = f.expressions[1]  # type e: FilterColumnExpression
        self.assertEqual(e.column_name, 'hybrid')
        self.assertEqual(e.column.key, 'hybrid')
        self.assertEqual(e.operator_str, '$eq')  # inserted
        self.assertEqual(e.value, True)
        self.assertIn('(a.id > 10 AND (EXISTS (SELECT 1', stmt2sql(e.compile_expression()))

        e = f.expressions[2]  # type e: FilterColumnExpression
        self.assertIsInstance(e, FilterColumnExpression)
        self.assertEqual(e.column_name, 'data.rating')
        self.assertEqual(e.column.key, None)  # it's a JSON expressin
        self.assertEqual(e.real_column.key, 'data')
        self.assertEqual(e.operator_str, '$eq')  # inserted
        self.assertEqual(e.value, 10)
        self.assertEqual(stmt2sql(e.compile_expression()), "CAST((a.data #>> ['rating']) AS INTEGER) = 10")  # proper typecasting

        # === Test: scalar operators
        f = MongoFilter(ManyFieldsModel).input(OrderedDict([
            ('a', {'$lt': 100}),
            ('b', {'$lte': 100}),
            ('c', {'$ne': 100}),
            ('d', {'$gte': 100}),
            ('e', {'$gt': 100}),
            ('f', {'$in': [1, 2, 3]}),
            ('g', {'$nin': [1, 2, 3]}),
            ('h', {'$exists': 1}),
            ('i', {'$exists': 0}),
        ]))

        self.assertEqual(len(f.expressions), 9)

        e = f.expressions[0]
        self.assertEqual(e.operator_str, '$lt')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.a < 100')

        e = f.expressions[1]
        self.assertEqual(e.operator_str, '$lte')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.b <= 100')

        e = f.expressions[2]
        self.assertEqual(e.operator_str, '$ne')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.c IS DISTINCT FROM 100')

        e = f.expressions[3]
        self.assertEqual(e.operator_str, '$gte')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.d >= 100')

        e = f.expressions[4]
        self.assertEqual(e.operator_str, '$gt')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.e > 100')

        e = f.expressions[5]
        self.assertEqual(e.operator_str, '$in')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.f IN (1, 2, 3)')

        e = f.expressions[6]
        self.assertEqual(e.operator_str, '$nin')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.g NOT IN (1, 2, 3)')

        e = f.expressions[7]
        self.assertEqual(e.operator_str, '$exists')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.h IS NOT NULL')

        e = f.expressions[8]
        self.assertEqual(e.operator_str, '$exists')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.i IS NULL')

        # === Test: array operators
        f = MongoFilter(ManyFieldsModel).input(OrderedDict([
            ('aa', {'$eq': 1}),
            ('bb', {'$eq': [1, 2, 3]}),
            ('cc', {'$ne': 1}),
            ('dd', {'$ne': [1, 2, 3]}),
            ('ee', {'$in': [1, 2, 3]}),
            ('ff', {'$nin': [1, 2, 3]}),
            ('gg', {'$exists': 1}),
            ('hh', {'$exists': 0}),
            ('ii', {'$all': [1, 2, 3]}),
            ('jj', {'$size': 0}),
            ('kk', {'$size': 99}),
        ]))

        self.assertEqual(len(f.expressions), 11)

        e = f.expressions[0]
        self.assertEqual(e.operator_str, '$eq')
        self.assertEqual(stmt2sql(e.compile_expression()), '1 = ANY (m.aa)')

        e = f.expressions[1]
        self.assertEqual(e.operator_str, '$eq')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.bb = CAST(ARRAY[1, 2, 3] AS VARCHAR[])')

        e = f.expressions[2]
        self.assertEqual(e.operator_str, '$ne')
        self.assertEqual(stmt2sql(e.compile_expression()), '1 != ALL (m.cc)')

        e = f.expressions[3]
        self.assertEqual(e.operator_str, '$ne')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.dd != CAST(ARRAY[1, 2, 3] AS VARCHAR[])')

        e = f.expressions[4]
        self.assertEqual(e.operator_str, '$in')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.ee && CAST(ARRAY[1, 2, 3] AS VARCHAR[])')

        e = f.expressions[5]
        self.assertEqual(e.operator_str, '$nin')
        self.assertEqual(stmt2sql(e.compile_expression()), 'NOT m.ff && CAST(ARRAY[1, 2, 3] AS VARCHAR[])')

        e = f.expressions[6]
        self.assertEqual(e.operator_str, '$exists')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.gg IS NOT NULL')

        e = f.expressions[7]
        self.assertEqual(e.operator_str, '$exists')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.hh IS NULL')

        e = f.expressions[8]
        self.assertEqual(e.operator_str, '$all')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.ii @> CAST(ARRAY[1, 2, 3] AS VARCHAR[])')

        e = f.expressions[9]
        self.assertEqual(e.operator_str, '$size')
        self.assertEqual(stmt2sql(e.compile_expression()), 'array_length(m.jj, 1) IS NULL')

        e = f.expressions[10]
        self.assertEqual(e.operator_str, '$size')
        self.assertEqual(stmt2sql(e.compile_expression()), 'array_length(m.kk, 1) = 99')

        # === Test: operators on JSON columns
        f = MongoFilter(ManyFieldsModel).input(OrderedDict([
            ('j_a.rating', {'$lt': 100}),
            ('j_b.rating', {'$in': [1, 2, 3]}),
        ]))

        self.assertEqual(len(f.expressions), 2)

        e = f.expressions[0]
        self.assertEqual(e.operator_str, '$lt')
        self.assertEqual(stmt2sql(e.compile_expression()), "CAST((m.j_a #>> ['rating']) AS INTEGER) < 100")

        e = f.expressions[1]
        self.assertEqual(e.operator_str, '$in')
        self.assertEqual(stmt2sql(e.compile_expression()), "CAST((m.j_b #>> ['rating']) AS TEXT) IN (1, 2, 3)")

        # === Test: boolean expression
        f = MongoFilter(ManyFieldsModel).input({
            '$and': [
                OrderedDict([ ('a', 1), ('b', 2) ]),
                {'c': 3},
                {'g': {'$gt': 18}},
            ]
        })

        self.assertEqual(len(f.expressions), 1)

        e = f.expressions[0]
        self.assertIsInstance(e, FilterBooleanExpression)
        self.assertEqual(e.operator_str, '$and')

        self.assertIsInstance(e.value, list)
        self.assertIsInstance(e.value[0], list)
        self.assertEqual(stmt2sql(e.value[0][0].compile_expression()), 'm.a = 1')
        self.assertEqual(stmt2sql(e.value[0][1].compile_expression()), 'm.b = 2')
        self.assertIsInstance(e.value[1], list)
        self.assertEqual(stmt2sql(e.value[1][0].compile_expression()), 'm.c = 3')
        self.assertIsInstance(e.value[2], list)
        self.assertEqual(stmt2sql(e.value[2][0].compile_expression()), 'm.g > 18')
        self.assertEqual(stmt2sql(e.compile_expression()),
                         '((m.a = 1 AND m.b = 2) AND m.c = 3 AND m.g > 18)')

        f = MongoFilter(ManyFieldsModel).input({
            '$or': [
                {'a': 1},
                {'b': 1},
            ],
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         '(m.a = 1 OR m.b = 1)')

        f = MongoFilter(ManyFieldsModel).input({
            '$nor': [
                {'a': 1},
                {'b': 1},
            ],
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         'NOT (m.a = 1 OR m.b = 1)')

        f = MongoFilter(ManyFieldsModel).input({
            '$not': {
                'c': {'$gt': 18},
            }
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         'm.c <= 18')  # wow, clever sqlalchemy!

        # === Test: nested boolean expression
        f = MongoFilter(ManyFieldsModel).input({
            '$not': OrderedDict([
                ('a', 1),
                ('$and', [
                    {'a': 1},
                    {'b': 1},
                    {'$or': [
                        {'a': {'$gt': 18}},
                        {'b': 1},
                    ]}
                ]),
            ])
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         'NOT (m.a = 1 AND (m.a = 1 AND m.b = 1 AND (m.a > 18 OR m.b = 1)))')

        # === Test: related columns
        f = MongoFilter(Article).input(OrderedDict([
            # These two will be put together into a single subquery
            ('comments.id', 1),
            ('comments.uid', {'$gt': 18}),
            # These two will also be grouped
            ('user.id', 1),
            ('user.name', {'$nin': ['a', 'b']}),
        ]))

        self.assertEqual(len(f.expressions), 4)

        e = f.expressions[0]
        self.assertEqual(stmt2sql(e.compile_expression()), "c.id = 1")

        e = f.expressions[1]
        self.assertEqual(stmt2sql(e.compile_expression()), "c.uid > 18")

        e = f.expressions[2]
        self.assertEqual(stmt2sql(e.compile_expression()), "u.id = 1")

        e = f.expressions[3]
        self.assertEqual(stmt2sql(e.compile_expression()), "u.name NOT IN (a, b)")

        s = stmt2sql(f.compile_statement())
        # We rely on OrderedDict, so the order of arguments should be perfect
        self.assertIn(u"(EXISTS (SELECT 1 \n"
                         "FROM a, c \n"
                         "WHERE a.id = c.aid AND c.id = 1 AND c.uid > 18))", s)
        self.assertIn(u"(EXISTS (SELECT 1 \n"
                         "FROM u, a \n"
                         "WHERE u.id = a.uid AND u.id = 1 AND u.name NOT IN (a, b)))", s)

        # === Test: Hybrid Properties
        f = MongoFilter(Article).input(dict(hybrid=1))
        self.assertIn('(a.id > 10 AND (EXISTS (SELECT 1 \nFROM u', stmt2sql(f.compile_statement()))

        # === Test: dry run of compile_*()
        # No errors
        for input_value in (None, {'id': 1}):
            MongoFilter(Article).input(input_value).compile_statement()

    def test_limit(self):
        # Test: empty value
        l = MongoLimit(User).input()
        self.assertEqual((l.skip, l.limit), (None, None))

        # Test: skip
        l = MongoLimit(User).input(skip=10)
        self.assertEqual((l.skip, l.limit), (10, None))

        # Test: limit
        l = MongoLimit(User).input(limit=10)
        self.assertEqual((l.skip, l.limit), (None, 10))

        # Test: max_items
        l = MongoLimit(User, max_items=10).input()
        self.assertEqual((l.skip, l.limit), (None, 10))

        l = MongoLimit(User, max_items=10).input(limit=20)
        self.assertEqual((l.skip, l.limit), (None, 10))

        l = MongoLimit(User, max_items=10).input(limit=5)
        self.assertEqual((l.skip, l.limit), (None, 5))

    def test_join(self):
        def test_mjp(mjp, relname, qo):
            self.assertEqual(mjp.relationship_name, relname)
            self.assertEqual(mjp.query_object, qo)

        def test_mongojoin(mongojoin, *expected_mjps):
            self.assertEqual(len(mongojoin.mjps), len(expected_mjps))
            for mjp, expected_mjp in zip(mongojoin.mjps, expected_mjps):
                test_mjp(mjp, **expected_mjp)

        mq = MongoQuery(User)
        mj = Reusable(MongoJoin(User).with_mongoquery(mq))  # type: MongoJoin

        # === Test: empty value
        test_mongojoin(mj.input(None))
        test_mongojoin(mj.input(()))
        test_mongojoin(mj.input([]))
        test_mongojoin(mj.input({}))

        # === Test: list
        j = mj.input(('articles',))
        test_mongojoin(j, dict(relname='articles', qo=None))

        # Test: dict + None
        j = mj.input({'articles': None})
        test_mongojoin(j, dict(relname='articles', qo=None))

        # === Test: dict + empty dict
        j = mj.input({'articles': {}})
        test_mongojoin(j, dict(relname='articles', qo=None))

        # === Test: dict + dict
        j = mj.input({'articles': dict(project=('id',))})
        test_mongojoin(j, dict(relname='articles', qo=dict(project=('id',))))

        # === Test: dict + dict + dict
        j = mj.input({'articles': dict(project=('id',),
                                       join={
                                           'comments': dict(project=('id',))
                                       })})
        test_mongojoin(j, dict(relname='articles', qo=dict(project=('id',),
                                                           join={'comments': dict(project=('id',))})))



        # === Test: merge()
        # Test plain relations as a list
        j = mj.input(('articles',))

        j.merge(('articles',))
        j.merge(('articles',))  # no problem twice
        self.assertEqual(j.get_projection_tree(), {'articles': {}})

        j.merge(('comments',))
        self.assertEqual(j.get_projection_tree(), {'articles': {}, 'comments': {}})

        # Test plain relations with a nested projection
        j = mj.input({'articles': dict(project=('title',))})

        j.merge(('articles',))
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1}})  # no change

        j.merge({'articles': dict(project=('data',))})
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1, 'data': 1}})  # + 'data'

        # Test plain, nested join, projections
        j = mj.input({'articles': dict(project=('title',),
                                       join={
                                           'comments': dict(project=('text',))
                                       })})

        j.merge({'articles': dict(project=('data',),
                                  join={
                                      'user': dict(project=('id',)),
                                      'comments': dict(project=('id',),
                                                       join=('user',))
                                  })})
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1, 'data': 1,  # + 'data'
                                                                'comments': {'text': 1, 'id': 1,  # +'id'
                                                                             # + 'user':
                                                                             'user': {}
                                                                             },
                                                                # + 'user':
                                                                'user': {'id': 1}
                                                                }})

        # Test: conflicting merge
        with self.assertRaises(InvalidQueryError):
            # Can't merge with a filter
            j.merge({'articles': dict(filter={'id': 1})})

        # Test: quietly
        j = mj.input({'articles': dict(project=('title',))})

        j.merge(('comments',), quietly=True)
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1}})  # no 'comments'

        j.merge({'articles': dict(project=('data',))}, quietly=True)
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1, 'data': 0}})  # no 'data'



        # === Test: allowed_relations
        mj = Reusable(MongoJoin(User, allowed_relations=('articles',)).with_mongoquery(mq))  # type: MongoJoin

        mj.input(('articles',))
        with self.assertRaises(DisabledError):
            mj.input(('comments',))
        with self.assertRaises(InvalidRelationError):
            mj.input(('non-existent',))

        # === Test: banned_relations
        mj = Reusable(MongoJoin(User, banned_relations=('comments',)).with_mongoquery(mq))  # type: MongoJoin

        mj.input(('articles',))
        with self.assertRaises(DisabledError):
            mj.input(('comments',))
        with self.assertRaises(InvalidRelationError):
            mj.input(('non-existent',))

        # Test: allowed_relations + banned_relations
        with self.assertRaises(AssertionError):
            Reusable(MongoJoin(User, allowed_relations=('articles',), banned_relations=('comments',)).with_mongoquery(mq))


    def test_mongoquery_pluck_instance(self):
        """ Test MongoQuery.pluck_instance() """
        # === Test: pluck one user
        # This is all about projections
        u = User(id=1, name='a', tags=[], age=18)

        mq = User.mongoquery().query(project=['name'])
        self.assertEqual(mq.pluck_instance(u), dict(name='a'))

        mq = User.mongoquery().query(project=['name', 'user_calculated'])
        self.assertEqual(mq.pluck_instance(u), dict(name='a', user_calculated=28))

        mq = User.mongoquery().query(project={'tags': 0})
        self.assertEqual(mq.pluck_instance(u), dict(id=1, name='a', age=18, user_calculated=28))

        # === Test: pluck user, articles
        # Now we have a join to a one-to-many relationship.
        # 'join' handler is supposed to run a nested plucking session
        u = User(id=1, name='a', tags=[], age=18)
        u.articles = [
            Article(id=1, uid=1, title='a', theme='s', data={}, user=u),
            Article(id=2, uid=1, title='b', theme='s', data={}, user=u),
            Article(id=3, uid=1, title='c', theme='s', data={}, user=u),
        ]

        # Plain join. No restrictions.
        mq = User.mongoquery().query(project=['name'],
                                     join=('articles',))
        self.assertEqual(mq.pluck_instance(u),
                         dict(name='a',
                              articles=[
                                  # Everything
                                  dict(id=1, uid=1, title='a', theme='s', data={}, calculated=2, hybrid=False),
                                  dict(id=2, uid=1, title='b', theme='s', data={}, calculated=2, hybrid=False),
                                  dict(id=3, uid=1, title='c', theme='s', data={}, calculated=2, hybrid=False),
                              ]))

        # Join with projection
        mq = User.mongoquery().query(project=['name'],
                                     join={'articles': dict(project=('title',))})
        self.assertEqual(mq.pluck_instance(u),
                         dict(name='a',
                              articles=[
                                  # Just one prop
                                  dict(title='a'),
                                  dict(title='b'),
                                  dict(title='c'),
                              ]))

        # Join, two levels, projections
        # It also uses Artiles.user, which is a one-to-one relationship
        # It also uses 'joinf' to make sure it's also involved
        mq = User.mongoquery().query(project=['name'],
                                     join={'articles': dict(project=('title',),
                                                            joinf={'user': dict(project=('id',))}
                                                            )})
        self.assertEqual(mq.pluck_instance(u),
                         dict(name='a',
                              articles=[
                                  # one prop + user
                                  dict(title='a', user=dict(id=1)),
                                  dict(title='b', user=dict(id=1)),
                                  dict(title='c', user=dict(id=1)),
                              ]))

        # === Test: pluck user, articles, user
        # A join to a one-to-one relationship
        # 'join' handler is supposed to handle it.
        a = u.articles[0]
        mq = Article.mongoquery().query(project=['title'],
                                        join={'user': dict(project=['name'])})
        self.assertEqual(mq.pluck_instance(a),
                         dict(title='a', user=dict(name='a')))

        # === Test: won't pluck a wrong object
        with self.assertRaises(ValueError):
            mq.pluck_instance(u)  # can pluck Article, not User

    # NOTE: we don't test 'join', 'aggregate', 'limit', 'count' here, because they're tested in t3_statements_test.py
