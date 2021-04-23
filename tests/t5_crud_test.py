
import unittest
from typing import Callable

from flask import Flask, g
from flask_jsontools import FlaskJsonClient, DynamicJSONEncoder
from sqlalchemy.orm.exc import NoResultFound

from . import models
from .crud_view import ArticleView, GirlWatcherView
from mongosql import StrictCrudHelper, StrictCrudHelperSettingsDict, saves_relations, ABSENT


class CrudTestBase(unittest.TestCase):
    def setUp(self):
        # Init db
        self.engine, self.Session = models.get_working_db_for_tests()
        self.db = self.Session()
        self.db.begin()

        # Flask
        self.app = app = Flask(__name__)
        app.debug = app.testing = True
        app.json_encoder = DynamicJSONEncoder
        app.test_client_class = FlaskJsonClient

        ArticleView.route_as_view(app, 'articles', ('/article/', '/article/<int:id>'))
        GirlWatcherView.route_as_view(app, 'girlwatchers', ('/girlwatcher/', '/girlwatcher/<int:id>'))

        @app.before_request
        def db():
            g.db = self.db

    def tearDown(self):
        self.db.close()  # Reset session


class ArticleViewTest(CrudTestBase):
    """ Test ArticleView """
    def test_crudhelper(self):
        """ Test crudhelper configuration """
        make_crudhelper = lambda **kw: StrictCrudHelper(
            models.Article,
            **StrictCrudHelperSettingsDict(
                **kw
            )
        )

        # === Test: ro_fields
        ch = make_crudhelper(
            ro_fields=('id',),  # everything else must be RW
        )
        self.assertEqual(ch.ro_fields, {'id'})
        self.assertEqual(ch.rw_fields, {'uid', 'title', 'theme', 'data', 'calculated'})
        self.assertEqual(ch.const_fields, set())

        # === Test: defaults to all fields writable
        ch = StrictCrudHelper(models.Article)
        self.assertEqual(ch.ro_fields, set())
        self.assertEqual(ch.rw_fields, {'id', 'uid', 'title', 'theme', 'data', 'calculated'})
        self.assertEqual(ch.const_fields, set())

        # === Test: ro_fields=()
        ch = make_crudhelper(
            ro_fields=(),  # everything is RW
        )
        self.assertEqual(ch.ro_fields, set())
        self.assertEqual(ch.rw_fields, {'id', 'uid', 'title', 'theme', 'data', 'calculated'})
        self.assertEqual(ch.const_fields, set())

        # === Test: rw_fields
        ch = make_crudhelper(
            rw_fields=('data',),  # everything else must be RO
        )
        self.assertEqual(ch.ro_fields, {'id', 'uid', 'title', 'theme',
                                        # also properties and relationships
                                        'calculated', 'comments', 'hybrid', 'user'})
        self.assertEqual(ch.rw_fields, {'data'})
        self.assertEqual(ch.const_fields, set())

        # === Test: rw_fields=()
        ch = make_crudhelper(
            rw_fields=(),  # everything is RO
        )
        self.assertEqual(ch.ro_fields, {'id', 'uid', 'title', 'theme', 'data',
                                        # also properties and relationships
                                        'calculated', 'comments', 'hybrid', 'user'
                                        })
        self.assertEqual(ch.rw_fields, set())
        self.assertEqual(ch.const_fields, set())

        # === Test: const_fields
        ch = make_crudhelper(
            const_fields=('uid',),  # everything else is rw
        )
        self.assertEqual(ch.ro_fields, set())
        self.assertEqual(ch.rw_fields, {'id', 'title', 'theme', 'data', 'calculated'})
        self.assertEqual(ch.const_fields, {'uid'})

        # === Test: const_fields & ro_fields
        ch = make_crudhelper(
            ro_fields=('id',),
            const_fields=('uid',),
            # everything else is rw
        )
        self.assertEqual(ch.ro_fields, {'id'})
        self.assertEqual(ch.rw_fields, {'title', 'theme', 'data', 'calculated'})  # no 'id'
        self.assertEqual(ch.const_fields, {'uid'})

        # === Test: const_fields & rw_fields
        ch = make_crudhelper(
            rw_fields=('title', 'theme', 'data'),
            const_fields=('uid',),
            # everything else is rw
        )
        self.assertEqual(ch.ro_fields, {'id',
                                        # also properties and relationships
                                        'calculated', 'comments', 'hybrid', 'user'
                                        })
        self.assertEqual(ch.rw_fields, {'title', 'theme', 'data'})  # no 'id'
        self.assertEqual(ch.const_fields, {'uid'})

        # === Test: legacy_fields
        ch = make_crudhelper(
            rw_fields=('title', 'theme', 'data'),
            const_fields=('uid',),
            legacy_fields=('removed_column',),
            # everything else is rw
        )
        self.assertEqual(ch.legacy_fields, {'removed_column'})

        # === Test: query_defaults
        ch = make_crudhelper(
            query_defaults=dict(
                join=('user',),
            )
        )

        # Default
        mq = ch._query_model({})
        self.assertEqual(mq.get_projection_tree(), {
            'calculated': 0,
            'hybrid': 0,
            'user': {'user_calculated': 0},
        })

        # Override
        mq = ch._query_model({'join': None})
        self.assertEqual(mq.get_projection_tree(), {
            'calculated': 0,
            'hybrid': 0,
            # no more 'user'
        })

    def test_list(self):
        """ Test list() """

        # Simple list
        # maxitems:2, sort:id- should apply
        with self.app.test_client() as c:
            rv = c.get('/article/', json=None)
            self.assertEqual(rv['articles'], [
                # 2 items
                # sort: id-
                {'id': 30, 'uid': 3, 'theme': None, 'title': '30', 'data': {'o': {'z': False}}},
                {'id': 21, 'uid': 2, 'theme': None, 'title': '21', 'data': {'rating': 4, 'o': {'z': True}}}
            ])

        # Query list
        # Try to override sort, limit
        with self.app.test_client() as c:
            rv = c.get('/article/', json={
                'query': {
                    'limit': 3, # Cannnot exceed
                    'sort': ['id+'],  # Sort changed
                    'project': ['id', 'uid']
                }})
            self.assertEqual(rv['articles'], [
                # Still 2 items: cannot exceed maxitems
                # sort: id+ (overridden)
                # Projection worked
                {'id': 10, 'uid': 1},
                {'id': 11, 'uid': 1},
            ])

        # Query list, aggregate
        with self.app.test_client() as c:
            rv = c.get('/article/', json={
                'query': {
                    'filter': {
                        'id': {'$gte': '10'},
                    },
                    'aggregate': {
                        'n': {'$sum': 1},
                        'sum_ids': {'$sum': 'id'},
                        'max_rating': {'$max': 'data.rating'},
                        'avg_rating': {'$avg': 'data.rating'},
                    },
                    'sort': None,  # Unset initial sorting. Otherwise, PostgreSQL wants this column in GROUP BY
                }})
            self.assertEqual(rv['articles'], [
                {
                    'n': 6,
                    'sum_ids': 10+11+12+20+21+30,
                    'max_rating': 6.0,
                    'avg_rating': (5+5.5+6+4.5+4  +0)/5,
                }
            ])

        # Test count
        with self.app.test_client() as c:
            rv = c.get('/article/', json={
                'query': {
                    'count': 1
                }})
            self.assertEqual(rv['articles'], 6)  # `max_rows` shouldn't apply here; therefore, we don't get a '2'

    def test_create(self):
        """ Test create() """
        article_json = {
            'id': 999, 'uid': 999,
            'title': '999',
            'theme': None,
            'data': {'wow': True}
        }

        expected_response_object = {
            'id': 1,  # Auto-set
            'uid': 3,  # Set manually
            'title': '999',
            'theme': None,
            'data': {'wow': True},
        }

        # Create
        # 'ro' field should be set manually
        with self.app.test_client() as c:
            rv = c.post('/article/', json={'article': article_json})
            self.assertEqual(rv['article'], expected_response_object)

            # Create: test that MongoSQL projections & joins are supported even when creating
            self.db.begin()  # the previous request has closed it
            rv = c.post('/article/', json={
                'article': article_json,
                'query': {
                    'project': ['title'],
                    'join': {'user': {'project': ['id', 'name']}}
                }
            })
            self.assertEqual(rv['article'], {'title': '999', 'user': {'id': 3, 'name': 'c'}})  # respects projections

            # Test: legacy_field
            self.db.begin()  # the previous request has closed it
            article_json['removed_column'] = 'something'  # legacy_column should be ignored
            expected_response_object['id'] = 3  # expecting it to be alright

            rv = c.post('/article/', json={'article': article_json})
            self.assertEqual(rv['article'], expected_response_object)  # same response; went ok

            # @saves_relations() called even though it's a legacy field
            self.assertEqual(ArticleView._save_removed_column, dict(removed_column='something'))

    def test_create__save_many(self):
        """ Test create() with submitting many objects at once """
        with self.app.test_client() as c:
            # === Test: 3 x create()
            # Submit 3 new objects
            res = c.post('/article/', json={'articles': [
                {'title': 'a'},
                {'title': 'b'},
                {'title': 'c'},
            ]}).get_json()
            self.assertNotIn('error', res)  # no generic error
            self.assertEqual(res['errors'], {})  # no individual errors

            # All saved just fine
            self.assertEqual(res['articles'], [
                # All saved fine
                {'id': 1, 'uid': 3, 'title': 'a', 'theme': None, 'data': None},
                {'id': 2, 'uid': 3, 'title': 'b', 'theme': None, 'data': None},
                {'id': 3, 'uid': 3, 'title': 'c', 'theme': None, 'data': None},
            ])

            # === Test: update, create, delegate
            self.db.begin()  # the previous request has closed it
            # Submit:
            #   1 new object
            #   1 updated object
            #   1 object with a custom PK but which is missing
            #   1 object with title='z': the view is programmed to raise an exception in this case :)
            res = c.post('/article/', json={'articles': [
                # save: 1 new object: no PK
                {'title': 'd'},
                # save: 1 updated object
                {'id': 1, 'title': 'A'},  # now uppercase
                # save: 1 object with a custom PK: will be ignored
                {'id': 9, 'title': 'e'},  # will be ignored
                # save: 1 object with error
                {'title': 'z'},
            ]}).get_json()

            self.assertNotIn('error', res)  # no generic error
            self.assertEqual(res['errors'], {
                '3': 'This method inexplicably fails when title="z"',
            })

            # Everything else is saved just fine
            self.assertEqual(res['articles'], [
                {'id': 4, 'uid': 3, 'title': 'd', 'theme': None, 'data': None},
                {'id': 1, 'uid': 3, 'title': 'A', 'theme': None, 'data': None},
                None,
                None,
            ])


    def test_get(self):
        """ Test get() """

        # Simple get
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['id', 'uid'],
                }
            })
            self.assertEqual(rv['article'], {
                'id': 30, 'uid': 3
            })

        self.db.close()  # Reset session and its cache

        # Query get: relations
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['id', 'uid'],
                    'join': ['user',],
                }
            })
            self.assertEqual(rv['article'], {
                'id': 30, 'uid': 3,
                'user': {
                    'id': 3,
                    'name': 'c',
                    'age': 16,
                    'age_in_10': 26,
                    'tags': ['3', 'a', 'b', 'c'],
                    'master_id': None,
                }
            })

        self.db.close()  # Reset session and its cache

        # Query get: relations with filtering, projection and further joins
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['id', 'uid'],
                    'join': {
                        'user': {
                            'project': ['name'],
                            'join': {
                                'comments': {
                                    'filter': {
                                        'uid': '3'
                                    }
                                }
                            },
                        }
                    }
                }
            })

            from pprint import pprint
            self.assertEqual(rv['article'], {
                'id': 30,
                'uid': 3,
                'user': {
                    'name': 'c',
                    'comments': [{'id': 102, 'uid': 3, 'aid': 10, 'text': '10-c', }]
                }
            })

        self.db.close()  # Reset session and its cache

    def test_update(self):
        """ Test update() """

        # Update
        # `uid` should be copied over
        # JSON `data` should be merged
        with self.app.test_client() as c:
            rv = c.post('/article/10', json={
                'article': {
                    'id': 999, 'uid': 999, # 'ro': ignored
                    'data': {'?': ':)'}
                }
            })
            self.assertEqual(rv['article'], {
                'id': 10,  # ro
                'uid': 1,  # ro
                'title': '10',  # Unchanged
                'theme': None,
                'data': {'?': ':)', 'o': {'a': True}, 'rating': 5},  # merged
            })

            # Update: respects MongoSQL projections & joins
            self.db.begin()  # the previous request has closed it
            rv = c.post('/article/10', json={
                'article': {},
                'query': {
                    'project': ['title'],
                }
            })
            self.assertEqual(rv['article'], {'title': '10'})

            # Test: legacy_field
            self.db.begin()  # the previous request has closed it
            rv = c.post('/article/10', json={
                'article': {'removed_column': 'something'},  # got to be ignored
            })
            self.assertNotIn('error', rv.get_json())

            # Test: update a @property
            self.db.begin()  # the previous request has closed it
            rv = c.post('/article/10', json={
                'article': {'calculated': '!!! :)'}
            }).get_json()
            self.assertEqual(rv['article']['title'], '10'+'!!! :)')

    def test_delete(self):
        """ Test delete() """

        # Delete
        with self.app.test_client() as c:
            rv = c.delete('/article/10', json=None)
            art = rv['article']
            art.pop('comments', None)
            self.assertEqual(rv['article'], {
                'id': 10, 'uid': 1,
                'title': '10',
                'theme': None,
                'data': {'o': {'a': True}, 'rating': 5},
            })

            self.db.close()

            self.assertRaises(NoResultFound, c.get, '/article/10')  # really removed

    def test_404(self):
        """ Try accessing entities that do not exist """

    def test_property_project(self):
        """ Test project of @property """

        # Simple get
        with self.app.test_client() as c:
            rv = c.get('/article/30', json={
                'query': {
                    'project': ['uid', 'calculated'],
                }
            })
            self.assertEqual(rv['article'], {
                'uid': 3, 'calculated': 5
            })
            rv = c.get('/article/', json={
                'query': {
                    'project': ['uid', 'calculated'],
                }
            })
            self.assertEqual(rv['articles'], [
                # 2 items
                # sort: id-
                {'uid': 3, 'calculated': 5},
                {'uid': 2, 'calculated': 4}
            ])
            # Propjection for join
            rv = c.get('/article/20', json={
                'query': {
                    'project': ['id'],
                    'join': {'comments': {
                        'project': ['id', 'comment_calc'],
                    }}}
            })
            self.assertEqual(rv['article'], {
                'id': 20,
                'comments': [
                    {'comment_calc': 'ONE', 'id': 106},
                    {'comment_calc': 'TWO', 'id': 107}]
            })

            try:
                rv = c.get('/article/', json={
                    'query': {
                        'project': ['uid', 'no_such_property'],
                    }
                })
                assert False, 'Should throw an exception'
            except:
                pass

    def test_saves_relations(self):
        """ Test how @saves_relations works """

        # === Test: ABSENT
        self.assertIsNotNone(ABSENT)
        self.assertFalse(ABSENT)
        self.assertTrue(ABSENT is ABSENT)
        self.assertEqual(str(ABSENT), '-')
        self.assertEqual(repr(ABSENT), '-')

        # === Test: @saves_relations
        # Test the behavior of the decorator on a custom view class
        class View:
            def __init__(self):
                self.log = set()

            # Will use this as a marker that no value was provided
            NOT_PROVIDED = '-'

            # simple: save once
            @saves_relations('a')
            def save_a(self, new, old=None, a=NOT_PROVIDED):
                self.log.add('save_a: new={new}, old={old}, a={a}'.format(**locals()))

            # save twice
            @saves_relations('a')
            def save_a_again(self, new, old=None, a=NOT_PROVIDED):
                self.log.add('save_a_again: new={new}, old={old}, a={a}'.format(**locals()))

            # save another
            @saves_relations('b')
            def save_b(self, new, old=None, b=NOT_PROVIDED):
                self.log.add('save_b: new={new}, old={old}, b={b}'.format(**locals()))

            # save both
            @saves_relations('a', 'b')
            def save_ab(self, new, old=None, a=NOT_PROVIDED, b=NOT_PROVIDED):
                self.log.add('save_ab: new={new}, old={old}, a={a}, b={b}'.format(**locals()))

        # Construct a view
        view = View()

        # Test: a descriptor can only be accessed through the dict
        # On a class
        self.assertIsInstance(View.save_a, saves_relations)
        self.assertIsInstance(View.save_a, Callable)
        self.assertIsInstance(View.__dict__['save_a'], saves_relations)
        # On an object
        self.assertNotIsInstance(view.save_a, saves_relations)
        self.assertIsInstance(view.save_a, Callable)

        # Test: getting the descriptor through the class
        self.assertIsInstance(View.save_a.saves_relations, saves_relations)
        self.assertIsInstance(saves_relations.get_method_decorator(View, 'save_a'), saves_relations)

        # Test: the descriptor returns the decorator, not the wrapped method
        self.assertIs(
            View.save_a,
            View.__dict__['save_a']#.method  # not anymore
        )

        # Test: collects decorators correctly
        decorators = saves_relations.all_decorators_from(View)
        self.assertEqual(
            {'save_a', 'save_a_again', 'save_b', 'save_ab'},
            {d.method_name for d in decorators}
        )

        # Test: collects relationship names correctly
        self.assertEqual(
            saves_relations.all_relation_names_from(View),
            {'a', 'b'}
        )

        # Fails with an object
        with self.assertRaises(ValueError):
            saves_relations.all_decorators_from(view)
        with self.assertRaises(ValueError):
            saves_relations.all_relation_names_from(view)

        # === Test: execute_handler_methods(), empty input
        view = View()
        saves_relations.execute_handler_methods(view, dict(), 'a', None)

        self.assertEqual(view.log, {
            # all handlers were executed,even though there was no input
            'save_a: new=a, old=None, a=-',
            'save_a_again: new=a, old=None, a=-',
            'save_ab: new=a, old=None, a=-, b=-',
            'save_b: new=a, old=None, b=-',
        })

        # === Test: execute_handler_methods(), 'a' provided
        view = View()
        saves_relations.execute_handler_methods(view, dict(a='A'), 'a', None)

        self.assertEqual(view.log, {
            # all handlers executed, 'a' now provided
            'save_a: new=a, old=None, a=A',
            'save_a_again: new=a, old=None, a=A',
            'save_ab: new=a, old=None, a=A, b=-',
            'save_b: new=a, old=None, b=-',
        })

        # === Test: @saves_relations actually handles requests
        with self.app.test_client() as c:
            # Post
            rv = c.post('/article/', json=dict(
                article=dict(uid=999, title='999',
                             # `user` provided, `comments` not provided
                             user=dict(example=1)
                             )))
            self.assertIn('article', rv.get_json())

            # See what happened
            self.assertEqual(ArticleView._save_comments__args['new'].title, '999')
            self.assertIsNone(ArticleView._save_comments__args['prev'])
            self.assertIsNone(ArticleView._save_comments__args['comments'])

            self.assertEqual(ArticleView._save_relations__args['new'].title, '999')
            self.assertIsNone(ArticleView._save_relations__args['prev'])
            self.assertIsNone(ArticleView._save_relations__args['comments'])
            self.assertEqual(ArticleView._save_relations__args['user'], dict(example=1))

        # === Test: on a class with __slots__
        class ViewSlots:
            __slots__ = ()

            @saves_relations('a')
            def save_a(self, new, old=None, a=None):
                pass

        view = ViewSlots()

        # Test: collects decorators correctly
        decorators = saves_relations.all_decorators_from(ViewSlots)
        self.assertEqual(
            {'save_a'},
            {d.method_name for d in decorators}
        )


class GirlWatcherViewTest(CrudTestBase):
    """ Test GirlWatcherView """

    def test_superficial_get(self):
        """ Quickly test the view just to make sure it does not err """
        with self.app.test_client() as c:
            rv = c.get('/girlwatcher/', json=None).get_json()
            self.assertIn('girlwatchers', rv)

            rv = c.get('/girlwatcher/1', json=None).get_json()
            self.assertIn('girlwatcher', rv)

