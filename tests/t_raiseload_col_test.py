import unittest

import pytest
from sqlalchemy.exc import NoSuchColumnError
from sqlalchemy.orm import Load
from sqlalchemy import exc as sa_exc

from .util import ExpectedQueryCounter

try:
    from mongosql import raiseload_col
except ImportError:
    raiseload_col = None

from . import models


class RaiseloadTesterMixin:
    def assertRaiseloadWorked(self, entity, loaded, raiseloaded, unloaded):
        """ Test columns and their load state

            :param entity: the entity
            :param loaded: column names that are loaded and may be accessed without emitting any sql queries
            :param raiseloaded: column names that will raise an InvalidRequestError when accessed
            :param unloaded: column names that will emit 1 query when accessed
        """
        # loaded
        for name in loaded:
            with ExpectedQueryCounter(self.engine, 0,
                                      'Unexpected query while accessing column {}'.format(name)):
                getattr(entity, name)

        # raiseloaded
        for name in raiseloaded:
            with self.assertRaises(sa_exc.InvalidRequestError, msg='Exception was not raised when accessing attr `{}`'.format(name)):
                getattr(entity, name)

        # unloaded
        for name in unloaded:
            with ExpectedQueryCounter(self.engine, 1,
                                      'Expected one query while accessing column {}'.format(name)):
                getattr(entity, name)


class RaiseloadColTest(RaiseloadTesterMixin, unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.engine, cls.Session = models.get_working_db_for_tests()

    def test_defer_pk(self):
        """ Test: we can't defer a PK """
        # Test: try to defer a PK with load_only()
        ssn = self.Session()
        u = ssn.query(models.User).options(
            Load(models.User).load_only(models.User.name),
        ).first()

        self.assertRaiseloadWorked(u,
                                   loaded={'id', 'name'},  # PK is still here
                                   raiseloaded={},
                                   unloaded={'age', 'tags'})

        # defer()'s default behavior has no failsafe mechanism: it will defer a PK
        ssn = self.Session()
        with self.assertRaises(NoSuchColumnError):
            u = ssn.query(models.User).options(
                Load(models.User).undefer(models.User.name),
                Load(models.User).defer('*'),  # as it happens, sqlalchemy will actually let us defer a PK! be careful!
            ).first()

    @pytest.mark.skipif(raiseload_col is None, reason='nplus1loader is not available')
    def test_raiseload_col(self):
        """ raiseload_col() on a single column """
        # raiseload_rel() some columns
        ssn = self.Session()
        u = ssn.query(models.User).options(
            Load(models.User).load_only(models.User.name),
            Load(models.User).raiseload_col(models.User.tags, models.User.age),
        ).first()

        self.assertRaiseloadWorked(u,
                                   loaded={'id', 'name'},
                                   raiseloaded={'age', 'tags'},
                                   unloaded={})

        # raiseload_col() on a PK destroys entity loading, and sqlalchemy gives an error
        with self.assertRaises(NoSuchColumnError):
            ssn = self.Session()
            u = ssn.query(models.User).options(
                Load(models.User).raiseload_col(models.User.id),
            ).first()

    @pytest.mark.skipif(raiseload_col is None, reason='nplus1loader is not available')
    def test_raiseload_star(self):
        """ raiseload_col('*') """
        ssn = self.Session()

        # raiseload_col() will defer our PKs!
        with self.assertRaises(NoSuchColumnError):
            u = ssn.query(models.User).options(
                Load(models.User).load_only(models.User.name),
                Load(models.User).raiseload_col('*')
            ).first()

        # Have to undefer() PKs manually
        u = ssn.query(models.User).options(
            Load(models.User).load_only(models.User.name),
            Load(models.User).undefer(models.User.id),  # undefer PK manually
            Load(models.User).raiseload_col('*')
        ).first()

        self.assertRaiseloadWorked(u,
                                   loaded={'id', 'name'},
                                   raiseloaded={'age', 'tags'},
                                   unloaded={})

    @pytest.mark.skipif(raiseload_col is None, reason='nplus1loader is not available')
    def test_interaction_with_other_options(self):
        # === Test: just load_only()
        # NOTE: we have to restart ssn = self.Session() every time because otherwise SqlAlchemy is too clever and caches entities in the session!!
        ssn = self.Session()
        user = ssn.query(models.User).options(
            Load(models.User).load_only('name', 'age'),  # only these two
        ).first()

        self.assertRaiseloadWorked(
            user,
            loaded={'id', 'name', 'age'},
            raiseloaded={},
            unloaded={'tags',
                        'articles', 'comments'}
        )

        # === Test: raiseload_rel()
        ssn = self.Session()
        user = ssn.query(models.User).options(
            Load(models.User).load_only('name', 'age'),  # only these two
            Load(models.User).undefer(models.User.id),  # undefer PK manually
            Load(models.User).raiseload('*'),
        ).first()

        self.assertRaiseloadWorked(
            user,
            loaded={'id', 'name', 'age'},
            raiseloaded={'articles', 'comments'},
            unloaded={'tags'}
        )

        # === Test: raiseload_rel() + raiseload_col()
        ssn = self.Session()
        user = ssn.query(models.User).options(
            Load(models.User).load_only('name', 'age'),  # only these two
            Load(models.User).undefer(models.User.id),  # undefer PK manually
            Load(models.User).raiseload_col('*'),
            Load(models.User).raiseload('*'),
        ).first()

        self.assertRaiseloadWorked(
            user,
            loaded={'id', 'name', 'age'},
            raiseloaded={'tags',
                         'articles', 'comments'},
            unloaded={}
        )

    # More tests in:
    # tests.t4_query_test.QueryTest#test_raise
