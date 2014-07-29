import unittest
from sqlalchemy import inspect

from . import models


class QueryTest(unittest.TestCase):
    """ Test MongoQuery """

    def setUp(self):
        # Connect, create tables
        engine, Session = models.init_database()
        models.drop_all(engine)
        models.create_all(engine)

        # Fill DB
        ssn = Session()
        ssn.add_all(models.content_samples())
        ssn.commit()

        # Session
        self.engine = engine
        self.db = ssn

        # Logging
        import logging
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    def tearDown(self):
        pass#models.drop_all(self.engine)  # FIXME: test hangs when dropping tables for the second time!

    def test_projection(self):
        ssn = self.db

        # Test: load only 2 props
        user = models.User.mongoquery(ssn).project(['id', 'name']).end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments'})

        # Test: load without 2 props
        user = models.User.mongoquery(ssn).project('-age,tags').end().first()
        self.assertEqual(inspect(user).unloaded, {'age', 'tags', 'articles', 'comments'})

    def test_sort(self):
        ssn = self.db

        # Test: sort(age+, id-)
        users = models.User.mongoquery(ssn).sort(['age+', 'id+']).end().all()
        self.assertEqual([3, 1, 2], [u.id for u in users])

    def test_filter(self):
        ssn = self.db

        # Test: filter(age=16)
        users = models.User.mongoquery(ssn).filter({'age': 16}).end().all()
        self.assertEqual([3], [u.id for u in users])

    def test_join(self):
        ssn = self.db

        # Test: no join(), relationships are unloaded
        user = models.User.mongoquery(ssn).end().first()
        self.assertEqual(inspect(user).unloaded, {'articles', 'comments'})

        # Test:    join(), relationships are   loaded
        user = models.User.mongoquery(ssn).join(['articles']).end().first()
        self.assertEqual(inspect(user).unloaded, {'comments'})

    def test_count(self):
        ssn = self.db

        # Test: count()
        n = models.User.mongoquery(ssn).count().end().scalar()
        self.assertEqual(3, n)

    @unittest.SkipTest
    def test_group(self):
        ssn = self.db

        # Test: sort(age+, id-)
        users = models.User.mongoquery(ssn).group(['age']).end().all()
        self.assertEqual(2, len(users))
        self.assertEqual({16, 18}, {u.age for u in users})
