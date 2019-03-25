import unittest
from sqlalchemy.orm import load_only
from sqlalchemy import exc as sa_exc

from mongosql import raiseload_col
from . import models


class RaiseloadColTest(unittest.TestCase):
    def setUp(self):
        self.engine, self.Session = models.get_working_db_for_tests()

    def test_raiseload_col(self):
        ssn = self.Session()

        u = ssn.query(models.User) \
            .options(load_only(models.User.id)) \
            .options(raiseload_col(models.User.tags, models.User.age)) \
            .first()

        self.assertEqual(set(u.__dict__.keys()), {'id', '_sa_instance_state'})

        u.id  # ok
        u.name  # ok

        with self.assertRaises(sa_exc.InvalidRequestError):
            u.tags  # raiseload_col() works here

        with self.assertRaises(sa_exc.InvalidRequestError):
            u.age  # raiseload_col() works here

    def test_raiseload_star(self):
        ssn = self.Session()

        u = ssn.query(models.User) \
            .options(load_only(models.User.id)) \
            .options(raiseload_col('*')) \
            .first()

        self.assertEqual(set(u.__dict__.keys()), {'id', '_sa_instance_state'})

        u.id  # ok
        with self.assertRaises(sa_exc.InvalidRequestError):
            u.name  # not available

        with self.assertRaises(sa_exc.InvalidRequestError):
            u.tags  # raiseload_col() works here

        with self.assertRaises(sa_exc.InvalidRequestError):
            u.age  # raiseload_col() works here
