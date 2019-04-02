import unittest

from sqlalchemy import inspect
from sqlalchemy.orm import Load

from mongosql import Reusable, MongoQuery, MongoQuerySettingsDict

from . import t_raiseload_col_test
from . import models
from .util import ExpectedQueryCounter


row2dict = lambda row: dict(zip(row.keys(), row))  # zip into a dict


class QueryTest(t_raiseload_col_test.RaiseloadTesterMixin, unittest.TestCase):
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
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments', 'roles'})

        # Test: load without 2 props
        user = models.User.mongoquery(ssn).query(project={'age': 0, 'tags': 0}).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments', 'roles'})

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
        self.assertEqual(inspect(user).unloaded, {'articles', 'comments', 'roles'})

        # Test:    join(), relationships are   loaded
        user = models.User.mongoquery(ssn).query(join=['articles']).end().first()
        self.assertEqual(inspect(user).unloaded, {'comments', 'roles'})

    def test_join_query(self):
        """ Test join(dict) """
        ssn = self.Session()

        # Test: join() with comments as dict
        user = models.User.mongoquery(ssn).query(filter={'id': 1},
                                                 join={'comments': None}).end().one()
        self.assertEqual(user.id, 1)
        self.assertEqual(inspect(user).unloaded, {'articles', 'roles'})

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
        self.assertEqual(inspect(user).unloaded, {'comments', 'roles'})
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
        self.assertEqual(inspect(user).unloaded, {'comments', 'roles'})
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
        n = models.User.mongoquery(ssn).query(count=True).end().scalar()
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
