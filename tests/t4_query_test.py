import unittest

import pytest
from sqlalchemy import inspect

from mongosql import Reusable, MongoQuery, MongoQuerySettingsDict

from . import t_raiseload_col_test
from . import models
from .util import QueryLogger, TestQueryStringsMixin


try:
    import nplus1loader
except ImportError:
    nplus1loader = None


row2dict = lambda row: dict(zip(row.keys(), row))  # zip into a dict


class QueryTest(t_raiseload_col_test.RaiseloadTesterMixin, TestQueryStringsMixin, unittest.TestCase):
    """ Test MongoQuery """

    # Enable SQL query logging
    SQL_LOGGING = False

    @classmethod
    def setUpClass(cls):
        # Init db
        cls.engine, cls.Session = models.get_working_db_for_tests()
        cls.db = cls.Session()

        # Logging
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO if cls.SQL_LOGGING else logging.ERROR)

    def test_projection(self):
        """ Test project() """
        ssn = self.db

        # Test: load only 2 props
        user = models.User.mongoquery(ssn).query(project=['id', 'name']).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'age_in_10', 'tags', 'articles', 'comments', 'roles', 'master_id', 'master'})
        ssn.expunge_all()

        # Test: load without 2 props
        user = models.User.mongoquery(ssn).query(project={'age': 0, 'tags': 0}).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments', 'roles', 'master'})
        ssn.expunge_all()

        # Test: load a deferred column_property()
        # Make sure it's not exluded (i.e. not in `unloaded`)
        user = models.User.mongoquery(ssn).query(project=['id', 'age_in_10']).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'name', 'tags', 'articles', 'comments', 'roles', 'master_id', 'master'})
        ssn.expunge_all()

    def test_sort(self):
        """ Test sort() """
        ssn = self.db

        # Test: sort(age+, id-)
        users = models.User.mongoquery(ssn).query(sort=['age+', 'id+']).end().all()
        self.assertEqual([3, 1, 2], [u.id for u in users])

    def test_filter(self):
        """ Test filter() """
        ssn = self.db

        # Test: filter(age=16)
        users = models.User.mongoquery(ssn).query(filter={'age': 16}).end().all()
        self.assertEqual([3], [u.id for u in users])

    def test_join(self):
        """ Test join() """
        ssn = self.db

        # Test: no join(), relationships are unloaded
        user = models.User.mongoquery(ssn).query().end().first()
        self.assertIn('articles', inspect(user).unloaded)
        self.assertIn('comments', inspect(user).unloaded)
        self.assertIn('roles', inspect(user).unloaded)

        # Test:    join(), relationships are   loaded
        user = models.User.mongoquery(ssn).query(join=['articles']).end().first()
        self.assertNotIn('articles', inspect(user).unloaded)  # now loaded
        self.assertIn('comments', inspect(user).unloaded)
        self.assertIn('roles', inspect(user).unloaded)

        # Test: join() to a legacy field that has `force_include=1` and faked with a @property
        mq = MongoQuery(models.User, MongoQuerySettingsDict(
            legacy_fields=('user_calculated',),
            force_include=('user_calculated',),
            bundled_project={
                'user_calculated': ['age'],
            }
        ))
        user = mq.with_session(ssn).query(project=['id'],
                                          join=['user_calculated']).end().first()
        self.assertEqual(mq.get_projection_tree(), dict(id=1, user_calculated=1))

    def test_join_query(self):
        """ Test join(dict) """
        ssn = self.Session()

        # Test: join() with comments as dict
        user = models.User.mongoquery(ssn).query(filter={'id': 1},
                                                 join={'comments': None}).end().one()
        self.assertEqual(user.id, 1)
        self.assertEqual(inspect(user).unloaded, {'articles', 'roles', 'master', 'age_in_10'})

        ssn.close() # need to reset the session: it caches entities and gives bad results

        # Test: join() with filtered articles
        user = models.User.mongoquery(ssn).query(
            filter={'id': 1},
            join={
                'articles': {
                    'project': ['id', 'title'],
                    'filter': {'id': 10},
                    'limit': 1
                }
            }
        ).end().one()

        self.assertEqual(user.id, 1)
        self.assertEqual(inspect(user).unloaded, {'comments', 'roles', 'master', 'age_in_10'})
        self.assertEqual([10], [a.id for a in user.articles])  # Only one article! :)
        self.assertEqual(inspect(user.articles[0]).unloaded, {'theme', 'user', 'comments',  'uid', 'data'})  # No relationships loaded, and projection worked

        # Test: complex nested joinf
        user = models.User.mongoquery(ssn).query(
            joinf={
                'articles': {
                    'project': ['id', 'title'],
                    'joinf': {
                        'comments': {
                            'project': ['id', 'text'],
                            'filter': {
                                'text': '20-a-ONE'
                            }
                        }
                    }
                }
            }
        ).end().one()

        self.assertEqual(user.id, 2)
        self.assertEqual(inspect(user).unloaded, {'comments', 'roles', 'master', 'age_in_10'})
        self.assertEqual([20], [a.id for a in user.articles])  # Only one article that has comment with text "20-a-ONE"
        article = user.articles[0]
        self.assertEqual(inspect(article).unloaded, {'theme', 'user', 'uid', 'data'})   # Only "comments" relationship is loaded
        self.assertEqual([106], [c.id for c in article.comments]) # Only the matching comment is present in the result
        comment = article.comments[0]
        self.assertEqual(inspect(comment).unloaded, {'uid', 'aid', 'user', 'article'})  # Only fields specified in the 'project' are loaded

    def test_count(self):
        """ Test count() """
        ssn = self.db

        # Test: count()
        q = models.User.mongoquery(ssn).query(count=True).end()
        n = q.scalar()
        self.assertEqual(3, n)

    def test_aggregate(self):
        """ Test aggregate() """
        ssn = self.db

        mq = Reusable(MongoQuery(models.User, MongoQuerySettingsDict(
            aggregate_columns=('age',),
            aggregate_labels=True
        )))

        mq_user = lambda: mq.with_session(ssn)

        # Test: aggregate()
        q = {
            'max_age': {'$max': 'age'},
            'adults': {'$sum': {'age': {'$gte': 18}}},
        }
        row = mq_user().query(aggregate=q).end().one()
        # type row: sqlalchemy.util.KeyedTuple
        self.assertEqual(row2dict(row), {'max_age': 18, 'adults': 2})

        # Test: aggregate { $sum: 1 }
        row = mq_user().query(aggregate={ 'n': {'$sum': 1} }).end().one()
        self.assertEqual(row.n, 3)

        # Test: aggregate { $sum: 10 }, with filtering
        row = mq_user().query(filter={'id': 1}, aggregate={'n': {'$sum': 10}}).end().one()
        self.assertEqual(row.n, 10)

        # Test: aggregate() & group()
        q = {
            'age': 'age',
            'n': {'$sum': 1},
        }
        rows = mq_user().query(aggregate=q, group=['age'], sort=['age-']).end().all()
        self.assertEqual([row2dict(r) for r in rows], [{'age': 18, 'n': 2}, {'age': 16, 'n': 1}])

    def test_json(self):
        """ Test operations on a JSON column """
        ssn = self.db

        # Filter: >=
        articles = models.Article.mongoquery(ssn).query(filter={ 'data.rating': {'$gte': 5.5} }).end().all()
        self.assertEqual({11, 12}, {a.id for a in articles})

        # Filter: == True
        articles = models.Article.mongoquery(ssn).query(filter={'data.o.a': True}).end().all()
        self.assertEqual({10, 11}, {a.id for a in articles})

        # Filter: is None
        articles = models.Article.mongoquery(ssn).query(filter={'data.o.a': None}).end().all()
        self.assertEqual({21, 30}, {a.id for a in articles})

        # Filter: wrong type, but still works
        articles = models.Article.mongoquery(ssn).query(filter={'data.rating': '5.5'}).end().all()
        self.assertEqual({11}, {a.id for a in articles})

        # Sort
        articles = models.Article.mongoquery(ssn).query(sort=['data.rating-']).end().all()
        self.assertEqual([None, 6, 5.5, 5, 4.5, 4], [a.data.get('rating', None) for a in articles])

        # Aggregate
        mq = Reusable(MongoQuery(models.Article, MongoQuerySettingsDict(
            aggregate_columns=('data', 'data'),
            aggregate_labels=True
        )))

        mq_article = lambda: mq.with_session(ssn)

        q = {
            'high': {'$sum': { 'data.rating': {'$gte': 5.0} }},
            'max_rating': {'$max': 'data.rating'},
            'a_is_none': {'$sum': { 'data.o.a': None } },
        }
        row = mq_article().query(aggregate=q).end().one()
        self.assertEqual(row2dict(row), {'high': 3, 'max_rating': 6, 'a_is_none': 2})

        # Aggregate & Group

    @pytest.mark.skipif(nplus1loader is None, reason='nplus1loader is not available')
    def test_raise(self):
        # Prepare settings
        user_settings = dict(
            raiseload=True,
            related={
                'articles': lambda: article_settings,
                'comments': lambda: comment_settings,
            }
        )
        article_settings = dict(
            raiseload=True,
            related={
                'user': lambda: user_settings,
                'comments': lambda: comment_settings,
            }
        )
        comment_settings = dict(
            raiseload=True,
            related={
                'user': lambda: user_settings,
                'article': lambda: article_settings
            }
        )

        # Prepare MongoQuery
        engine = self.engine

        mq_user = Reusable(MongoQuery(models.User, user_settings))
        mq_article = Reusable(MongoQuery(models.Article, article_settings))
        mq_comment = Reusable(MongoQuery(models.Comment, comment_settings))

        # === Test: User: no projections (all columns), no joins
        ssn = self.Session()
        user = mq_user.query().with_session(ssn).end().first()
        self.assertRaiseloadWorked(
            user,
            loaded={'id', 'name', 'age'},
            raiseloaded={'articles', 'comments', 'roles'},
            unloaded={}
        )

        # === Test: User, projection
        ssn = self.Session()
        user = mq_user.query(project=('name',)).with_session(ssn).end().first()
        self.assertRaiseloadWorked(
            user,
            loaded={'id', 'name'},  # PK is loaded
            raiseloaded={'age', 'tags',  # can't use these columns now
                         'articles', 'comments', 'roles'},
            unloaded={}
        )

        # === Test: User, projection + join Article
        ssn = self.Session()
        user = mq_user.query(project=('name',),
                             join=('articles',)).with_session(ssn).end().first()

        self.assertRaiseloadWorked(
            user,
            loaded={'id', 'name',  # PK is loaded
                        'articles',  # can use it now
                    },
            raiseloaded={'age', 'tags',
                         'comments', 'roles'},
            unloaded={}
        )

        article = user.articles[0]
        self.assertRaiseloadWorked(
            article,
            loaded={'id', 'uid', 'title', 'theme', 'data'},
            raiseloaded={'user', 'comments'},
            unloaded={}
        )

        # === Test: User, projection + join Article: projection
        ssn = self.Session()
        user = mq_user.query(project=('name',),
                             join={'articles': dict(project=('title',))}).with_session(ssn).end().first()

        user  # don't test user again ; it's pretty clear

        article = user.articles[0]
        self.assertRaiseloadWorked(
            article,
            loaded={'id', 'title',  # PK loaded
                    },
            raiseloaded={'uid', 'theme', 'data',  # columns raiseloaded
                         'user', 'comments'},
            unloaded={}
        )

        # === Test: User, projection + join Article: projection + join Comment: projection
        # TODO: FIXME: sqlalchemy 1.2.x: SAWarning: Column 'id' on table <selectable> being replaced by Column('id'),
        #  which has the same key. Consider use_labels for select() statements.
        ssn = self.Session()
        user = mq_user.query(project=('name',),
                             join={'articles': dict(project=('title',),
                                                    join={'comments': dict(project=('text',),
                                                                           join=('user',))})})\
            .with_session(ssn).end().first()

        article = user.articles[0]
        self.assertRaiseloadWorked(
            article,
            loaded={'id', 'title',  # PK loaded
                    'comments',  # relationship loaded
                    },
            raiseloaded={'uid', 'theme', 'data',  # columns raiseloaded
                         'user'},
            unloaded={}
        )

        comment = article.comments[0]
        self.assertRaiseloadWorked(
            comment,
            loaded={'id', 'text',  # PK loaded
                    'user'},
            raiseloaded={'aid', 'uid',  # columns raiseloaded
                         'article'},
            unloaded={}
        )

    def test_end_count(self):
        """ Test CountingQuery """

        m = models.Article

        # Get the actual count and the ids
        ssn = self.Session()
        COUNT = ssn.query(m).count()
        IDS = sorted([r[0] for r in ssn.query(m.id)])

        assert COUNT == 6  # we rely on this number

        resultIds = lambda results: sorted([r.id for r in results])

        # === Test: simple count: qc.count() and then qc.__iter()
        qc = m.mongoquery(ssn).query(project=('id',), sort=('id+',)).end_count()

        with QueryLogger(self.engine) as ql:
            # count is alright
            self.assertEqual(qc.count, COUNT)

            # results are alright
            self.assertListEqual(IDS, resultIds(list(qc)))

        # the query is what we expect
        self.assertEqual(len(ql), 1)
        self.assertQuery(ql[0],
                         'SELECT a.id AS a_id, count(*) OVER () AS anon_1',
                         'FROM a ORDER BY a.id')

        # === Test: simple count: qc.__iter(), and then qc.count()
        # These two scenarios are distinct:
        # sometimes we may get the count, and then iterate over the results, but
        # sometimes we may iterate first, and only then get the count.
        qc = m.mongoquery(ssn).query(project=('id',), sort=('id+',)).end_count()

        with QueryLogger(self.engine) as ql:
            # results are right
            self.assertListEqual(IDS, resultIds(list(qc)))

            # the count is right
            self.assertEqual(qc.count, COUNT)

        # the query is what we expect
        self.assertEqual(len(ql), 1)
        self.assertQuery(ql[0],
                         'SELECT a.id AS a_id, count(*) OVER () AS anon_1',
                         'FROM a ORDER BY a.id')

        # === Test: count with an offset
        qc = m.mongoquery(ssn).query(project=('id',), sort=('id+',),
                                     skip=1, limit=3  # limit, offset
                                     ).end_count()

        with QueryLogger(self.engine) as ql:
            # count is alright
            self.assertEqual(qc.count, COUNT)  # still the big total!

            # results are alright
            self.assertListEqual(IDS[1:4], resultIds(list(qc)))  # but the results are limited

        # Just one query
        self.assertEqual(len(ql), 1)

        # === Test: count with a large offset: an extra query has to be made
        qc = m.mongoquery(ssn).query(project=('id',), sort=('id+',),
                                     skip=9, limit=3  # skip everything
                                     ).end_count()

        with QueryLogger(self.engine) as ql:
            # count is alright
            self.assertEqual(qc.count, COUNT)  # still the big total!

            # results are alright
            self.assertListEqual([], resultIds(list(qc)))  # no results

        # Two queries were made this time
        self.assertEqual(len(ql), 2)
        # First query: attempted load
        self.assertQuery(ql[0],
                         'SELECT a.id AS a_id, count(*) OVER () AS anon_1',
                         'FROM a ORDER BY a.id',
                         'LIMIT 3 OFFSET 9')
        # Second query: count
        self.assertQuery(ql[1],
                         'SELECT count(*) AS count_1',
                         'FROM (SELECT a.id AS a_id',
                         'FROM a ORDER BY a.id) AS anon_1')

        # LIMIT and OFFSET were removed from the second query
        self.assertNotIn('OFFSET', ql[1])
        self.assertNotIn('LIMIT', ql[1])
