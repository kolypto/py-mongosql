import unittest
from collections import OrderedDict
from sqlalchemy.orm import Load, Query

from mongosql import Reusable, MongoQuery, ModelPropertyBags, MongoQuerySettingsDict
from mongosql.handlers import *
from mongosql.exc import InvalidColumnError, DisabledError, InvalidQueryError, InvalidRelationError
from mongosql.handlers.project import Default
from .models import *
from .util import stmt2sql


class HandlersTest(unittest.TestCase):
    """ Test individual handlers """

    longMessage = True
    maxDiff = None

    def test_projection(self):
        Article_project = lambda **kw: MongoProject(Article, ModelPropertyBags.for_model(Article), **kw)

        # Helpers to build full projections
        ALL_ARTICLE_FIELDS = {'id', 'uid', 'title', 'theme', 'data', 'calculated', 'hybrid'}
        ALL_EXCLUDED = dict.fromkeys(ALL_ARTICLE_FIELDS, 0)
        ALL_INCLUDED = dict.fromkeys(ALL_ARTICLE_FIELDS, 1)

        inc_all_except = lambda *kw: {**ALL_INCLUDED, **dict.fromkeys(kw, 0)}
        inc_none_but = lambda *kw: {**ALL_EXCLUDED, **dict.fromkeys(kw, 1)}

        def test_by_full_projection(p, **expected_full_projection):
            """ Test:
                * get_full_projection()
                * __contains__() of a projection using its full projection
                * compile_columns()
            """
            # Test: get_full_projection()
            try:
                self.assertEqual(p.get_full_projection(), expected_full_projection)
            except:
                print('Projection: ', p.projection)
                print('Full projection: Expected:', expected_full_projection)
                print('Full projection: Actual: ', p.get_full_projection())
                raise

            # Test properties: __contains__()
            for name, include in expected_full_projection.items():
                if include:
                    self.assertIn(name, p)
                else:
                    self.assertNotIn(name, p)

            # Test: compile_columns() only returns column properties
            columns = p.compile_columns()
            self.assertEqual(
                set(col.key for col in columns),
                set(col_name
                    for col_name in p.bags.columns.names
                    if expected_full_projection.get(col_name, 0))
            )

            # Test: projection
            if p.mode == p.MODE_MIXED:
                self.assertEqual(p.projection, expected_full_projection)
            elif p.mode == p.MODE_INCLUDE or p.mode == p.MODE_EXCLUDE:
                self.assertEqual(p.projection,
                                 {k: v for k, v in expected_full_projection.items()
                                  if (v == p.mode)})  # 1 if `MODE_INCLUDE`, 0 if `MODE_EXCLUDE`
            else:
                raise Exception('How did we get here?')


        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            Article_project().input(None).input(None)

        # === Test: No input
        p = Article_project().input(None)
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(calculated=0, hybrid=0))
        test_by_full_projection(p, **inc_all_except('calculated', 'hybrid'))

        # === Test: empty input
        p = Article_project().input([])
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict())
        test_by_full_projection(p, **ALL_EXCLUDED)

        # === Test: valid projection, string
        p = Article_project().input('id uid title')
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, uid=1, title=1))

        # === Test: Valid projection, array
        p = Article_project().input(['id', 'uid', 'title'])
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
        p = Article_project().input(dict(id=1, uid=1, title=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, uid=1, title=1))

        test_by_full_projection(p, # basically, the same thing
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=0, hybrid=0,
                                )

        # === Test: Valid projection, dict, exclude mode
        p = Article_project().input(dict(theme=0, data=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0, data=0, calculated=0, hybrid=0))

        test_by_full_projection(p,
                                id=1, uid=1, title=1,
                                theme=0, data=0,
                                calculated=0, hybrid=0,
                                )

        # === Test: `default_exclude` in exclude mode
        p = Article_project(default_exclude=('calculated', 'hybrid'))\
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

        # Make sure that Default() markers don't make it out of get_final_input_value()
        # If it does, jsonify() would fail
        def assert_no_Default_markers_in(d: dict):
            self.assertTrue(all(not isinstance(v, Default) for v in d.values()))
        assert_no_Default_markers_in(p.get_final_input_value())

        # === Test: `default_exclude` in include mode (no effect)
        p = Article_project(default_exclude=('calculated', 'hybrid')) \
            .input(dict(id=1, calculated=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, calculated=1))

        test_by_full_projection(p,
                                id=1, uid=0, title=0,
                                theme=0, data=0,
                                calculated=1, hybrid=0,  # one included, one excluded
                                )

        # === Test: `default_exclude_properties=True`, exclude mode
        p = Article_project(default_exclude_properties=True) \
            .input(dict(uid=0))

        self.assertEqual(p.default_exclude, {'calculated', 'hybrid'})
        test_by_full_projection(p,
                                id=1, uid=0, title=1,
                                theme=1, data=1,
                                calculated=0, hybrid=0,  # both excluded
                                )

        # === Test: `default_exclude_properties=True`, include mode
        p = Article_project(default_exclude_properties=True) \
            .input(dict(uid=1, calculated=1))

        test_by_full_projection(p,
                                id=0, uid=1, title=0,
                                theme=0, data=0,
                                calculated=1, hybrid=0,  # only the one explicitly required is included
                                )

        # === Test: `default_exclude_properties=False`, exclude mode
        # default_exclude_properties=True, exclude mode
        p = Article_project(default_exclude_properties=False) \
            .input(dict(uid=0, calculated=0))

        self.assertEqual(p.default_exclude, None)
        test_by_full_projection(p,
                                id=1, uid=0, title=1,
                                theme=1, data=1,
                                calculated=0, hybrid=1,  # 1 ex, 1 inc (like the rest of the columns)
                                )

        # === Test: `default_exclude_properties=False`, include mode
        p = Article_project(default_exclude_properties=False) \
            .input(dict(uid=1, calculated=1))

        test_by_full_projection(p,
                                id=0, uid=1, title=0,
                                theme=0, data=0,
                                calculated=1, hybrid=0,  # 1 inc, 1 exc (like the rest of the columns)
                                )

        # === Test: `default_unexclude_properties`, exclude mode
        p = Article_project(default_unexclude_properties=('calculated',)) \
            .input(dict(uid=0))

        self.assertEqual(p.default_exclude, {'hybrid'})
        test_by_full_projection(p,
                                id=1, uid=0, title=1,
                                theme=1, data=1,
                                calculated=1, hybrid=0,  # only one is included
                                )

        # === Test: `default_unexclude_properties`, include mode
        p = Article_project(default_unexclude_properties=('calculated',)) \
            .input(dict(uid=1))

        test_by_full_projection(p,
                                id=0, uid=1, title=0,
                                theme=0, data=0,
                                calculated=0, hybrid=0,  # behaves like a column
                                )

        # === Test: ensure_loaded
        pr = Reusable(Article_project(ensure_loaded=('uid',)))

        p = pr.input(['id'])
        self.assertEqual(p.get_full_projection()['uid'], 0)  # not included into the projecttion
        self.assertIn('uid', p)  # but loaded nevertheless

        p = pr.input({'uid': 0})
        self.assertEqual(p.get_full_projection()['uid'], 0)  # not included
        self.assertIn('uid', p)  # but loaded

        # === Test: default_projection
        pr = Reusable(Article_project(default_projection=dict(id=1, title=1)))

        # `None` uses the default projection
        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))
        test_by_full_projection(p, **inc_none_but('id', 'title'))

        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))
        test_by_full_projection(p, **inc_none_but('id', 'title'))

        # Empty values give no columns: override
        p = pr.input([])
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict())
        test_by_full_projection(p, **ALL_EXCLUDED)

        p = pr.input({})
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict())
        test_by_full_projection(p, **ALL_EXCLUDED)

        # === Test: default_projection: empty
        # The desired behavior is to have no fields included by default, when the default_projection is empty, not None.
        pr = Reusable(Article_project(default_projection=()))

        # `None` uses the default projection, which is nothing
        p = pr.input(None)
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict())
        test_by_full_projection(p, **ALL_EXCLUDED)

        # Empty values give no columns (same result, actually)
        p = pr.input([])
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict())
        test_by_full_projection(p, **ALL_EXCLUDED)

        p = pr.input({})
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict())
        test_by_full_projection(p, **ALL_EXCLUDED)

        # === Test: merge

        # Test everything with : id, uid, title
        # Mapped to `mode`
        project_id_uid_title_to = lambda mode: Article_project().input(dict.fromkeys(['id', 'uid', 'title'], mode))

        # Originally include, merge include
        pr = project_id_uid_title_to(1).merge(dict(data=1))
        self.assertEqual(pr.mode, pr.MODE_INCLUDE)
        self.assertEqual(pr.projection, dict(id=1, uid=1, title=1,
                                             # One new appended
                                             data=1))
        test_by_full_projection(pr, **inc_none_but('id', 'uid', 'title', 'data'))

        # Originally exclude, merge exclude
        pr = project_id_uid_title_to(0).merge(dict(data=0))
        self.assertEqual(pr.mode, pr.MODE_EXCLUDE)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0, calculated=0, hybrid=0,
                                             # One new appended
                                             data=0))
        test_by_full_projection(pr, **inc_none_but('theme'))

        # Originally include, merge exclude (conflict, just drop banned keys)
        pr = project_id_uid_title_to(1).merge(dict(uid=0))
        self.assertEqual(pr.mode, pr.MODE_INCLUDE)
        self.assertEqual(pr.projection, dict(id=1, title=1))
        test_by_full_projection(pr, **inc_none_but('id', 'title'))

        # Originally exclude, merge include (conflict, results in full projection)
        pr = project_id_uid_title_to(0).merge(dict(data=1))
        self.assertEqual(pr.mode, pr.MODE_MIXED)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0,
                                             # Full projection in mixed mode
                                             theme=1, data=1, calculated=0, hybrid=0))
        test_by_full_projection(pr, **inc_none_but('theme', 'data'))

        # Originally mixed, merge include
        pr = project_id_uid_title_to(0).merge(dict(data=1)).merge(dict(hybrid=1))
        self.assertEqual(pr.mode, pr.MODE_MIXED)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0,
                                             theme=1, data=1, calculated=0,
                                             # Now included
                                             hybrid=1))
        test_by_full_projection(pr, **inc_none_but('theme', 'data', 'hybrid'))

        # Originally mixed, merge exclude
        pr.merge(dict(hybrid=0))
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0,
                                             theme=1, data=1, calculated=0,
                                             # Now excluded again
                                             hybrid=0))
        test_by_full_projection(pr, **inc_none_but('theme', 'data'))

        # Originally include, merge mixed
        pr = project_id_uid_title_to(1).merge(dict(id=0, uid=1, title=0, theme=1, data=0, calculated=1, hybrid=0))
        self.assertEqual(pr.projection, inc_none_but('uid', 'theme', 'calculated'))
        test_by_full_projection(pr, **inc_none_but('uid', 'theme', 'calculated'))

        # Originally exclude, merge mixed
        pr = project_id_uid_title_to(0).merge(dict(id=0, uid=1, title=0, theme=1, data=0, calculated=1, hybrid=0))
        self.assertEqual(pr.projection, inc_none_but('uid', 'theme', 'calculated'))
        test_by_full_projection(pr, **inc_none_but('uid', 'theme', 'calculated'))

        # === Test: merge, quiet mode
        # Originally include, merge include
        pr = project_id_uid_title_to(1).merge(dict(data=1), quietly=True)
        self.assertEqual(pr.projection, dict(id=1, uid=1, title=1))  # not data
        self.assertEqual(pr.get_full_projection(),
                         dict(id=1, uid=1, title=1, theme=0, data=0, calculated=0, hybrid=0))  # not 'data'
        self.assertIn('data', pr)  # included quietly, and the `in` test will tell!

        # Originally exclude, merge include (conflict, results in full projection)
        pr = project_id_uid_title_to(0).merge(dict(title=1), quietly=True)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0, data=1, theme=1, calculated=0, hybrid=0))
        self.assertEqual(pr.get_full_projection(),
                         dict(id=0, uid=0, title=0, theme=1, data=1, calculated=0, hybrid=0))  # not 'title'
        self.assertIn('title', pr)  # quietly

        # Originally mixed, merge include
        pr = project_id_uid_title_to(0).merge(dict(data=1)).merge(dict(hybrid=1), quietly=True)
        self.assertEqual(pr.projection, dict(id=0, uid=0, title=0, theme=1, data=1, calculated=0, hybrid=0))  # not 'hybrid'

        # === Test: merge quietly, then override publicly
        pr = project_id_uid_title_to(1).merge(dict(data=1), quietly=True)
        self.assertEqual(pr.projection, dict(id=1, uid=1, title=1))  # not data

        # now override
        pr = pr.merge(dict(data=1), quietly=False)
        #self.assertEqual(pr.projection, dict(id=1, uid=1, title=1, data=1))  # now data is publicly merged  # TODO: not working!

        # === Test: force_include
        pr = Reusable(Article_project(force_include=('id',)))

        # Include mode
        p = pr.input(dict(title=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1, title=1))  # id force included
        test_by_full_projection(p, **inc_none_but('id', 'title'))
        # Exclude mode
        p = pr.input(dict(data=0))
        self.assertEqual(p.mode, p.MODE_MIXED)
        self.assertEqual(p.projection, dict(id=1,  # force included
                                            uid=1, title=1, theme=1,
                                            data=0,  # excluded by request
                                            calculated=0, hybrid=0))
        test_by_full_projection(p, **inc_all_except('data', 'calculated', 'hybrid'))

        # === Test: force_exclude
        pr = Reusable(Article_project(force_exclude=('data',)))
        # Include mode
        p = pr.input(dict(id=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1))  # no `data`
        test_by_full_projection(p, **inc_none_but('id'))
        # Include mode: same property
        p = pr.input(dict(id=1, data=1))
        self.assertEqual(p.mode, p.MODE_INCLUDE)
        self.assertEqual(p.projection, dict(id=1))  # No more data, even though requested
        test_by_full_projection(p, **inc_none_but('id'))
        # Exclude mode
        p = pr.input(dict(theme=0))
        self.assertEqual(p.mode, p.MODE_EXCLUDE)
        self.assertEqual(p.projection, dict(theme=0,  # excluded by request
                                            data=0,  # force excluded
                                            calculated=0, hybrid=0
                                            ))
        test_by_full_projection(p, **inc_all_except('theme', 'data', 'calculated', 'hybrid'))

        # === Test: merge() cannnot override force_include and force_exclude
        # force_include
        pr = Reusable(Article_project(force_include=('id',)))

        p = pr.input(dict(id=0))
        self.assertIn('id', p)  # can't undo
        test_by_full_projection(p, **inc_all_except('calculated', 'hybrid'))

        p = p.merge(dict(id=0))
        self.assertIn('id', p)  # can't undo
        test_by_full_projection(p, **inc_all_except('calculated', 'hybrid'))

        # force_exclude
        pr = Reusable(Article_project(force_exclude=('data',)))

        p = pr.input(dict(data=1))
        self.assertNotIn('data', p)  # can't undo
        test_by_full_projection(p, **ALL_EXCLUDED)

        p = p.merge(dict(data=0))
        self.assertNotIn('data', p)  # can't undo
        test_by_full_projection(p, **ALL_EXCLUDED)

        # === Test: bundled_project
        pr = Reusable(Article_project(bundled_project={'calculated': ['title', 'uid']}))
        p = pr.input(dict(calculated=1))
        self.assertIn('calculated', p)
        self.assertIn('title', p)  # the bundled field is actually loaded ...
        self.assertIn('uid', p)
        self.assertEqual(p.get_full_projection(), inc_none_but('calculated'))  # ... but quietly; not included into the projection
        self.assertEqual(p.projection, dict(calculated=1))

        # === Test: bundled_project, relationships
        mq = MongoQuery(Article, dict(bundled_project={'calculated': ['comments']}))
        mq = mq.query(project=dict(calculated=1))
        self.assertEqual(mq.get_projection_tree(), dict(calculated=1, comments=dict(comment_calc=0)))
        self.assertEqual(p.projection, dict(calculated=1))

        # === Test: bundled_project, force_include
        mq = MongoQuery(Article, dict(bundled_project={'calculated': ['title', 'id']}, force_include=('calculated',)))
        mq = mq.query(project=dict(data=1))
        self.assertEqual(mq.get_projection_tree(), dict(
            calculated=1,  # the force-included field
            #id=0, title=0,  # dependencies not included into the projection ...
            data=1,  # asked by the user
        ))
        self.assertIn('id', mq)  # ... but are loaded
        self.assertIn('title', mq)
        self.assertIn('data', mq)

        # === Test: bundled_project, default_exclude, default_unexclude_properties
        # A property is unexcluded and bundled
        pr = Reusable(Article_project(
            bundled_project={'calculated': ['title']},
            default_unexclude_properties=('calculated',),
        ))

        # Include all
        p = pr.input(dict(id=1, title=1, calculated=1))
        test_by_full_projection(p, **inc_none_but('id', 'title', 'calculated'))

        # Default: inc all, nothing quietly
        p = pr.input(None)
        # 'title' will be included, not quietly, but boldly.. :)
        # There was a bug that an attribute was included quietly even if it was explicitly requested
        test_by_full_projection(p, **inc_all_except('hybrid'))

        # === Test: bundled_project, default_exclude, default_unexclude_properties
        # Let's assume that `title` is a fat field, and the `calculated` property exposes the relevant part of it.
        # To save traffic, we hide `title` by default, and expose `calculated` by default
        pr = Reusable(Article_project(
            bundled_project={'calculated': ['title']},
            default_exclude=('title',),
            default_unexclude_properties=('calculated',),
        ))

        # By default, everything's included, except `title`
        # However, it gets quietly included because of `default_unexclude_properties`
        p = pr.input(None)
        self.assertIn('calculated', p)
        self.assertIn('title', p)  # quietly included
        self.assertEqual(p.get_full_projection(), inc_all_except('title', 'hybrid'))
        self.assertEqual(p.projection, inc_all_except('title', 'hybrid'))  # this is a quirk; projection = full projection. It does not have to; can be just dict(calculated=1)

        # The user excluded everything but `id`, so both `calculated` and `title` are out
        p = pr.input(dict(id=1))
        self.assertNotIn('calculated', p)
        self.assertNotIn('title', p)
        self.assertEqual(p.get_full_projection(), inc_none_but('id'))
        self.assertEqual(p.projection, dict(id=1))

        # === Test: Invalid projection, dict, problem: invalid arguments passed to __init__()
        with self.assertRaises(InvalidColumnError):
            Article_project(default_projection=dict(id=1, INVALID=1))
        with self.assertRaises(InvalidQueryError):
            Article_project(default_projection=dict(id=1, title=0))  # incomplete

        with self.assertRaises(InvalidColumnError):
            Article_project(default_exclude='id')
        with self.assertRaises(InvalidColumnError):
            Article_project(default_exclude=('INVALID',))

        with self.assertRaises(InvalidColumnError):
            Article_project(force_exclude='id')
        with self.assertRaises(InvalidColumnError):
            Article_project(force_exclude=('INVALID',))

        with self.assertRaises(InvalidColumnError):
            Article_project(force_include='id')
        with self.assertRaises(InvalidColumnError):
            Article_project(force_include=('INVALID',))

        # === Test: Invalid projection, dict, problem: 1s and 0s
        pr = Reusable(Article_project())

        with self.assertRaises(InvalidQueryError):
            pr.input(dict(id=1, title=0))

        # === Test: Invalid projection, dict, problem: invalid column
        with self.assertRaises(InvalidColumnError):
            pr.input(dict(INVALID=1))

        # === Test: A mixed object is only acceptable when it mentions EVERY column
        # No error
        Article_project().input(dict(id=1, uid=1, title=1, theme=1, data=0,
                                         calculated=1, hybrid=1))

        # === Test: pluck_instance()
        a = Article(id=100, uid=10, title='title', theme='theme', data=dict(a=1), user=User(age=21))
        pr = Article_project().input(dict(id=1, uid=1, calculated=1))

        test_by_full_projection(pr, **inc_none_but('id', 'uid', 'calculated'))
        d = pr.pluck_instance(a)
        self.assertEqual(d, dict(id=100, uid=10, calculated=15))

        # === Test: dry run of compile_*()
        # No errors
        for input_value in (None, ('id',), {'id': 1}, {'id': 0}):
            Article_project().input(input_value).compile_options(Load(Article))

    def test_sort(self):
        Article_sort = lambda: MongoSort(Article, ModelPropertyBags.for_model(Article))
        sr = Reusable(Article_sort())

        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            Article_sort().input(None).input(None)

        # === Test: no input
        s = sr.input(None)
        self.assertEqual(s.sort_spec, OrderedDict())

        # === Test: list
        s = sr.input(['id', 'uid+', 'title-'])
        self.assertEqual(s.sort_spec, OrderedDict([('id', +1),('uid', +1),('title', -1)]))

        # === Test: string
        s = sr.input('id uid+ title-')
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

        # === Test: merge()
        s = sr.input(['id']).merge(['uid+'])
        self.assertEqual(s.sort_spec, OrderedDict([('id', +1), ('uid', +1)]))

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
            Article_sort().input(input_value).compile_columns()

    def test_group(self):
        Article_group = lambda: MongoGroup(Article, ModelPropertyBags.for_model(Article))

        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            Article_group().input(None).input(None)

        # === Test: list
        g = Article_group().input(['uid'])
        self.assertEqual(g.group_spec, OrderedDict(uid=+1))

        g = Article_group().input(['uid-'])
        self.assertEqual(g.group_spec, OrderedDict(uid=-1))

        # We don't test much, because this `group` operation is essentially the same with `sort`,
        # and `sort` is already tested

    def test_filter(self):
        Article_filter = lambda **kw: MongoFilter(Article, ModelPropertyBags.for_model(Article))

        # === Test: input() can be called only once
        with self.assertRaises(RuntimeError):
            Article_filter().input(None).input(None)

        # === Test: empty
        f = Article_filter().input(None)  # no problem

        # === Test: simple key=value object
        f = Article_filter().input(OrderedDict([
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
        ManyFieldsModel_filter = lambda: MongoFilter(ManyFieldsModel, ModelPropertyBags.for_model(ManyFieldsModel))
        f = ManyFieldsModel_filter().input(OrderedDict([
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
        self.assertEqual(stmt2sql(e.compile_expression(), literal=True), 'm.f IN (1, 2, 3)')

        e = f.expressions[6]
        self.assertEqual(e.operator_str, '$nin')
        self.assertEqual(stmt2sql(e.compile_expression(), literal=True), 'm.g NOT IN (1, 2, 3)')

        e = f.expressions[7]
        self.assertEqual(e.operator_str, '$exists')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.h IS NOT NULL')

        e = f.expressions[8]
        self.assertEqual(e.operator_str, '$exists')
        self.assertEqual(stmt2sql(e.compile_expression()), 'm.i IS NULL')

        # === Test: array operators
        f = ManyFieldsModel_filter().input(OrderedDict([
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

        # === Test: operators on JSON columns, 1st level
        f = ManyFieldsModel_filter().input(OrderedDict([
            ('j_a.rating', {'$lt': 100}),
            ('j_b.rating', {'$in': [1, 2, 3]}),
        ]))

        self.assertEqual(len(f.expressions), 2)

        e = f.expressions[0]
        self.assertEqual(e.operator_str, '$lt')
        self.assertEqual(stmt2sql(e.compile_expression()), "CAST((m.j_a #>> ['rating']) AS INTEGER) < 100")

        e = f.expressions[1]
        self.assertEqual(e.operator_str, '$in')
        self.assertEqual(stmt2sql(e.compile_expression(), literal=True), "CAST((m.j_b #>> '{rating}') AS INTEGER) IN (1, 2, 3)")

        # === Test: operators on JSON columns, 2nd level
        f = ManyFieldsModel_filter().input(OrderedDict([
            ('j_a.embedded.field', {'$eq': 'hey'}),
        ]))

        self.assertEqual(len(f.expressions), 1)

        e = f.expressions[0]
        self.assertEqual(e.operator_str, '$eq')
        self.assertEqual(stmt2sql(e.compile_expression()), "CAST((m.j_a #>> ['embedded', 'field']) AS TEXT) = hey")

        # === Test: boolean expression
        f = ManyFieldsModel_filter().input({
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

        f = ManyFieldsModel_filter().input({
            '$or': [
                {'a': 1},
                {'b': 1},
            ],
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         '(m.a = 1 OR m.b = 1)')

        f = ManyFieldsModel_filter().input({
            '$nor': [
                {'a': 1},
                {'b': 1},
            ],
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         'NOT (m.a = 1 OR m.b = 1)')

        f = ManyFieldsModel_filter().input({
            '$not': {
                'c': {'$gt': 18},
            }
        })
        self.assertEqual(stmt2sql(f.compile_statement()),
                         'm.c <= 18')  # wow, clever sqlalchemy!

        # === Test: nested boolean expression
        f = ManyFieldsModel_filter().input({
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
        f = Article_filter().input(OrderedDict([
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
        self.assertEqual(stmt2sql(e.compile_expression(), literal=True), "u.name NOT IN ('a', 'b')")

        s = stmt2sql(f.compile_statement(), literal=True)
        # We rely on OrderedDict, so the order of arguments should be perfect
        self.assertIn("(EXISTS (SELECT 1 \n"
                        "FROM a, c \n"
                        "WHERE a.id = c.aid AND c.id = 1 AND c.uid > 18))", s)
        self.assertIn("(EXISTS (SELECT 1 \n"
                        "FROM u, a \n"
                        "WHERE u.id = a.uid AND u.id = 1 AND u.name NOT IN ('a', 'b')))", s)

        # === Test: Hybrid Properties
        f = Article_filter().input(dict(hybrid=1))
        self.assertIn('(a.id > 10 AND (EXISTS (SELECT 1 \nFROM u', stmt2sql(f.compile_statement()))

        # === Test: merge
        f = Article_filter().input(dict(id=1)).merge(dict(uid=2))
        q_str = stmt2sql(f.compile_statement())
        self.assertIn('(a.id = 1 AND a.uid = 2)', q_str)

        # === Test: dry run of compile_*()
        # No errors
        for input_value in (None, {'id': 1}):
            Article_filter().input(input_value).compile_statement()

    def test_limit(self):
        User_limit = lambda **kw: MongoLimit(User, ModelPropertyBags.for_model(User), **kw)

        # Test: empty value
        l = User_limit().input()
        self.assertEqual((l.skip, l.limit), (None, None))

        # Test: skip
        l = User_limit().input(skip=10)
        self.assertEqual((l.skip, l.limit), (10, None))

        # Test: limit
        l = User_limit().input(limit=10)
        self.assertEqual((l.skip, l.limit), (None, 10))

        # Test: max_items
        l = User_limit(max_items=10).input()
        self.assertEqual((l.skip, l.limit), (None, 10))

        l = User_limit(max_items=10).input(limit=20)
        self.assertEqual((l.skip, l.limit), (None, 10))

        l = User_limit(max_items=10).input(limit=5)
        self.assertEqual((l.skip, l.limit), (None, 5))

    def test_join(self):
        User_join = lambda **kw: MongoJoin(User, ModelPropertyBags.for_model(User), **kw)

        def test_mjp(mjp, relname, qo):
            self.assertEqual(mjp.relationship_name, relname)
            self.assertEqual(mjp.query_object, qo)

        def test_mongojoin(mongojoin, *expected_mjps):
            self.assertEqual(len(mongojoin.mjps), len(expected_mjps))
            for mjp, expected_mjp in zip(mongojoin.mjps, expected_mjps):
                test_mjp(mjp, **expected_mjp)

        mq = MongoQuery(User)
        mj = Reusable(User_join().with_mongoquery(mq))  # type: MongoJoin

        # === Test: empty value
        test_mongojoin(mj.input(None))
        test_mongojoin(mj.input(()))
        test_mongojoin(mj.input([]))
        test_mongojoin(mj.input({}))

        # === Test: list
        j = mj.input(('articles',))
        test_mongojoin(j, dict(relname='articles', qo=None))

        # === Test: string
        j = mj.input('articles')
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
        self.assertEqual(j.get_projection_tree(), {'articles': {'hybrid': 0, 'calculated': 0}})

        j.merge(('comments',))
        self.assertEqual(j.get_projection_tree(), {'articles': {'hybrid': 0, 'calculated': 0},
                                                   'comments': {'comment_calc': 0}})

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
                                                                             'user': {'user_calculated': 0}
                                                                             },
                                                                # + 'user':
                                                                'user': {'id': 1}
                                                                }})

        # Test: conflicting merge
        with self.assertRaises(InvalidQueryError):
            # Can't merge with a filter in strict mode
            j.merge({'articles': dict(filter={'id': 1})}, strict=True)

        # Test: conflicting merge, non-strict mode
        j.merge({'articles': dict(filter={'id': 1})}, strict=False)  # ok

        # Test: quietly
        j = mj.input({'articles': dict(project=('title',))})

        j.merge(('comments',), quietly=True)
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1}})  # no 'comments'

        j.merge({'articles': dict(project=('data',))}, quietly=True)
        self.assertEqual(j.get_projection_tree(), {'articles': {'title': 1}})  # no 'data'



        # === Test: allowed_relations
        mj = Reusable(User_join(allowed_relations=('articles',)).with_mongoquery(mq))  # type: MongoJoin

        mj.input(('articles',))
        with self.assertRaises(DisabledError):
            mj.input(('comments',))
        with self.assertRaises(InvalidRelationError):
            mj.input(('non-existent',))

        # === Test: banned_relations
        mj = Reusable(User_join(banned_relations=('comments',)).with_mongoquery(mq))  # type: MongoJoin

        mj.input(('articles',))
        with self.assertRaises(DisabledError):
            mj.input(('comments',))
        with self.assertRaises(InvalidRelationError):
            mj.input(('non-existent',))

        # Test: allowed_relations + banned_relations
        with self.assertRaises(ValueError):
            Reusable(User_join(allowed_relations=('articles',), banned_relations=('comments',)).with_mongoquery(mq))

    def test_projection_join(self):
        """ Test loading relationships by specifying their name in the projection """
        u = User

        # === Test: project column + relationship
        mq = u.mongoquery().query(
            project=['name', 'articles'],
        )
        self.assertEqual(mq.get_projection_tree(), {'name': 1, 'articles': {'calculated': 0, 'hybrid': 0}})


        # === Test: can specify relationships in MongoProject settings
        mq = MongoQuery(u, dict(force_include=('articles',))).query(project=['name',])
        self.assertEqual(mq.get_projection_tree(), {'name': 1, 'articles': {'calculated': 0, 'hybrid': 0}})

        # === Test: force_include a relationship cannot be overridden
        mq = MongoQuery(u, dict(force_include=('articles',))).query(project={'name': 1, 'articles': 0})
        self.assertEqual(mq.get_projection_tree(), {'name': 1, 'articles': {'calculated': 0, 'hybrid': 0}})

        # === Test: can specify relationships in default projection
        mq = MongoQuery(u, dict(default_projection={'articles': 1})).query()
        self.assertEqual(mq.get_projection_tree(), {'articles': {'calculated': 0, 'hybrid': 0}})

    def test_mongoquery_pluck_instance(self):
        """ Test MongoQuery.pluck_instance() """
        # === Test: pluck one user
        # This is all about projections
        u = User(id=1, name='a', tags=[], age=18, age_in_10=28)

        mq = User.mongoquery().query(project=['name'])
        self.assertEqual(mq.pluck_instance(u), dict(name='a'))

        mq = User.mongoquery().query(project=['name', 'user_calculated'])
        self.assertEqual(mq.pluck_instance(u), dict(name='a', user_calculated=28))

        mq = User.mongoquery().query(project={'tags': 0})
        self.assertEqual(mq.pluck_instance(u), dict(id=1, name='a', age=18, age_in_10=28, master_id=None))  # note: no @property!

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
        print('#'*20)
        mq = User.mongoquery().query(project=['name'],
                                     join=('articles',))
        self.assertEqual(mq.pluck_instance(u),
                         dict(name='a',
                              articles=[
                                  # Everything
                                  # Note: no @property, no @hybrid_property!
                                  dict(id=1, uid=1, title='a', theme='s', data={}),
                                  dict(id=2, uid=1, title='b', theme='s', data={}),
                                  dict(id=3, uid=1, title='c', theme='s', data={}),
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

    def test_legacy_fields(self):
        """ Test legacy_fields, with different handlers """
        LEGACY_NAME = 'removed-field'
        legacy_fields = (LEGACY_NAME,)

        init_handler = lambda Handler, legacy_fields=(): \
            Handler(Article,
                    ModelPropertyBags.for_model(Article),
                    legacy_fields=legacy_fields)

        # === Test: project
        with self.assertRaises(InvalidColumnError):
            init_handler(MongoProject).input([LEGACY_NAME])

        mq = init_handler(MongoProject, legacy_fields).input([LEGACY_NAME])  # ok
        self.assertEqual(mq.projection[LEGACY_NAME], 1)
        self.assertEqual(mq.get_full_projection()[LEGACY_NAME], 1)
        self.assertTrue(LEGACY_NAME in mq)  # remembered
        mq.alter_query(Query(Article), Load(Article))  # query okay


        # === Test: sort
        with self.assertRaises(InvalidColumnError):
            init_handler(MongoSort).input([LEGACY_NAME])

        mq = init_handler(MongoSort, legacy_fields).input([LEGACY_NAME])  # ok
        self.assertEqual(mq.sort_spec, {'removed-field': 1})  # still there
        mq.alter_query(Query(Article), Load(Article))  # query okay

        # === Test: group
        with self.assertRaises(InvalidColumnError):
            init_handler(MongoGroup).input([LEGACY_NAME])

        mq = init_handler(MongoGroup, legacy_fields).input([LEGACY_NAME])  # ok
        self.assertEqual(mq.group_spec, {'removed-field': 1})  # still there
        mq.alter_query(Query(Article), Load(Article))  # query okay

        # === Test: filter
        with self.assertRaises(InvalidColumnError):
            init_handler(MongoFilter).input({LEGACY_NAME: 1})

        init_handler(MongoFilter, legacy_fields).input({LEGACY_NAME: 1})  # ok
        mq = init_handler(MongoFilter, legacy_fields).input({'$or': [
            {LEGACY_NAME: 1}  # nested
        ]})  # ok
        # is ignored in the filter
        mq.alter_query(Query(Article), Load(Article))  # query okay

        # === Test: join
        mq = MongoQuery(Article)
        with self.assertRaises(InvalidColumnError):
            init_handler(MongoJoin).with_mongoquery(mq).input([LEGACY_NAME])

        mq = init_handler(MongoJoin, legacy_fields).with_mongoquery(mq).input([LEGACY_NAME])  # ok  # type: MongoJoin
        self.assertEqual(mq.projection[LEGACY_NAME], 1)
        self.assertEqual(mq.get_full_projection()[LEGACY_NAME], 1)
        self.assertEqual(mq.get_projection_tree()[LEGACY_NAME], 1)
        self.assertEqual(mq.get_full_projection_tree()[LEGACY_NAME], 1)
        self.assertTrue(LEGACY_NAME in mq)
        mq.alter_query(Query(Article), Load(Article))  # query okay

        # === Test: aggregate
        mq = MongoQuery(Article, MongoQuerySettingsDict(legacy_fields=legacy_fields))
        mq = init_handler(MongoAggregate, legacy_fields).with_mongoquery(mq).input({
            'label': {'$avg': LEGACY_NAME},
        })  # ok
        mq.alter_query(Query(Article), Load(Article))  # query okay


        # === Test: MongoQuery
        mq = Reusable(MongoQuery(Article, MongoQuerySettingsDict(legacy_fields=legacy_fields)))  # type: MongoQuery

        mq.query(project={LEGACY_NAME: 1}).end()
        mq.query(sort=[LEGACY_NAME+'-']).end()
        mq.query(sort=[LEGACY_NAME+'.field']).end()
        mq.query(filter={LEGACY_NAME: 1}).end()
        mq.query(filter={LEGACY_NAME+'.field': 1}).end()
        mq.query(join={LEGACY_NAME: dict(filter={'a': 1})}).end()
        mq.query(joinf={LEGACY_NAME: dict(filter={'a': 1})}).end()
        mq.query(aggregate={'label': {'$avg': LEGACY_NAME}}).end()
        mq.query(aggregate={'label': {'$avg': LEGACY_NAME+'.field'}}).end()
        mq.query(aggregate={'label': {'$sum': {LEGACY_NAME: 1}}}).end()
        mq.query(aggregate={'label': {'$sum': {LEGACY_NAME+'.field': 1}}}).end()
        
        # === Test: what happens if an existing field is both "legacy" and "force_include"?
        LEGACY_NAME = 'calculated'  # let's assume that it used to be a relationship, but is now a @property that 
        # fakes it
        mq = Reusable(MongoQuery(Article, MongoQuerySettingsDict(
            legacy_fields=(LEGACY_NAME,),  # let's assume that `calculated` used to be a "join"
            force_include=(LEGACY_NAME,),  # always include it
        )))  # type: MongoQuery

        # try projecting it
        q = mq.query(project=[LEGACY_NAME,])
        self.assertEqual(q.get_full_projection_tree()['calculated'], 1)
    
        # try joining it
        q = mq.query(join=[LEGACY_NAME])
        self.assertEqual(q.get_full_projection_tree()['calculated'], 1)
        
        # try joining it with a filter
        # the fake field won't actually filter, and even ignore the nonexistent field
        q = mq.query(join={LEGACY_NAME: dict(filter=dict(NONEXISTENT_FIELD=1))})
        self.assertEqual(q.get_full_projection_tree()['calculated'], 1)

