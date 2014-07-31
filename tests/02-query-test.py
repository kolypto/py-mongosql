import unittest
from sqlalchemy import inspect

from . import models


row2dict = lambda row: dict(zip(row.keys(), row))  # zip into a dict


class QueryTest(unittest.TestCase):
    """ Test MongoQuery """

    def setUp(self):
        # Connect, create tables
        engine, Session = models.init_database()
        models.drop_all(engine)
        models.create_all(engine)

        # Fill DB
        ssn = Session()
        ssn.begin()
        ssn.add_all(models.content_samples())
        ssn.commit()

        # Session
        self.Session = Session
        self.engine = engine
        self.db = Session()

        # Logging
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    def tearDown(self):
        self.db.close()  # Need to close the session: otherwise, drop_all() hangs forever
        models.drop_all(self.engine)

    def test_projection(self):
        """ Test project() """
        ssn = self.db

        # Test: load only 2 props
        user = models.User.mongoquery(ssn).project(['id', 'name']).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments'})

        # Test: load without 2 props
        user = models.User.mongoquery(ssn).project({'age': 0, 'tags': 0}).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments'})

    def test_sort(self):
        """ Test sort() """
        ssn = self.db

        # Test: sort(age+, id-)
        users = models.User.mongoquery(ssn).sort(['age+', 'id+']).end().all()
        self.assertEqual([3, 1, 2], [u.id for u in users])

    def test_filter(self):
        """ Test filter() """
        ssn = self.db

        # Test: filter(age=16)
        users = models.User.mongoquery(ssn).filter({'age': 16}).end().all()
        self.assertEqual([3], [u.id for u in users])

    def test_join(self):
        """ Test join() """
        ssn = self.db

        # Test: no join(), relationships are unloaded
        user = models.User.mongoquery(ssn).end().first()
        self.assertEqual(inspect(user).unloaded, {'articles', 'comments'})

        # Test:    join(), relationships are   loaded
        user = models.User.mongoquery(ssn).join(['articles']).end().first()
        self.assertEqual(inspect(user).unloaded, {'comments'})

    def test_join_query(self):
        """ Test join(dict) """
        ssn = self.Session()

        # Test: join() with comments as dict
        user = models.User.mongoquery(ssn)\
            .filter({'id': 1})\
            .join({
                'comments': None
            })\
            .end().one()
        self.assertEqual(user.id, 1)
        self.assertEqual(inspect(user).unloaded, {'articles'})

        ssn.close() # need to reset the session: it caches entities and gives bad results

        # Test: join() with filtered articles
        user = models.User.mongoquery(ssn) \
            .filter({'id': 1}) \
            .join({
                'articles': {
                    'project': ['id', 'title'],
                    'filter': {'id': 10},
                    'limit': 1
                }
            }) \
            .end().one()
        self.assertEqual(user.id, 1)
        self.assertEqual(inspect(user).unloaded, {'comments'})
        self.assertEqual([10], [a.id for a in user.articles])  # Only one article! :)
        self.assertEqual(inspect(user.articles[0]).unloaded, {'user', 'comments',  'uid', 'data'})  # No relationships loaded, and projection worked

    def test_count(self):
        """ Test count() """
        ssn = self.db

        # Test: count()
        n = models.User.mongoquery(ssn).count().end().scalar()
        self.assertEqual(3, n)

    def test_aggregate(self):
        """ Test aggregate() """
        ssn = self.db

        # Test: aggregate()
        q = {
            'max_age': {'$max': 'age'},
            'adults': {'$sum': {'age': {'$gte': 18}}},
        }
        row = models.User.mongoquery(ssn).aggregate(q).end().one()
        ':type row: sqlalchemy.util.KeyedTuple'
        self.assertEqual(row2dict(row), {'max_age': 18, 'adults': 2})

        # Test: aggregate { $sum: 1 }
        row = models.User.mongoquery(ssn).aggregate({ 'n': {'$sum': 1} }).end().one()
        self.assertEqual(row.n, 3)

        # Test: aggregate { $sum: 10 }, with filtering
        row = models.User.mongoquery(ssn).filter({'id': 1}).aggregate({'n': {'$sum': 10}}).end().one()
        self.assertEqual(row.n, 10)

        # Test: aggregate() & group()
        q = {
            'age': 'age',
            'n': {'$sum': 1},
        }
        rows = models.User.mongoquery(ssn).aggregate(q).group(['age']).sort(['age-']).end().all()
        self.assertEqual(map(row2dict, rows), [ {'age': 18, 'n': 2}, {'age': 16, 'n': 1} ])

    def test_json(self):
        """ Test operations on a JSON column """
        ssn = self.db

        # Filter: >=
        articles = models.Article.mongoquery(ssn).filter({ 'data.rating': {'$gte': 5.5} }).end().all()
        self.assertEqual({11, 12}, {a.id for a in articles})

        # Filter: == True
        articles = models.Article.mongoquery(ssn).filter({'data.o.a': True}).end().all()
        self.assertEqual({10, 11}, {a.id for a in articles})

        # Filter: is None
        articles = models.Article.mongoquery(ssn).filter({'data.o.a': None}).end().all()
        self.assertEqual({21, 30}, {a.id for a in articles})

        # Filter: wrong type, but still works
        articles = models.Article.mongoquery(ssn).filter({'data.rating': '5.5'}).end().all()
        self.assertEqual({11}, {a.id for a in articles})

        # Sort
        articles = models.Article.mongoquery(ssn).sort(['data.rating-']).end().all()
        self.assertEqual([None, 6, 5.5, 5, 4.5, 4], [a.data.get('rating', None) for a in articles])

        # Aggregate
        q = {
            'high': {'$sum': { 'data.rating': {'$gte': 5.0} }},
            'max_rating': {'$max': 'data.rating'},
            'a_is_none': {'$sum': { 'data.o.a': None } },
        }
        row = models.Article.mongoquery(ssn).aggregate(q).end().one()
        self.assertEqual(row2dict(row), {'high': 3, 'max_rating': 6, 'a_is_none': 2})

        # Aggregate & Group
