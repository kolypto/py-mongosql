import unittest
from random import shuffle
from sqlalchemy.orm import defaultload, selectinload

from . import models
from .util import QueryLogger, TestQueryStringsMixin
from .saversion import SA_SINCE, SA_UNTIL
from mongosql import selectinquery


# Detect SqlAlchemy version
# We need to differentiate, because:
# in 1.2.x, selectinload() builds a JOIN query from the left entity to the right entity
# in 1.3.x, selectinload() queries just the right entity, and filters by the foreign key field directly
from .saversion import SA_12, SA_13, SA_14, SA_SINCE, SA_UNTIL


class SelectInQueryLoadTest(unittest.TestCase, TestQueryStringsMixin):
    @classmethod
    def setUpClass(cls):
        cls.engine, cls.Session = models.get_working_db_for_tests()
        cls.ssn = cls.Session()  # let every test reuse the same session; expect some interference issues

    def test_filter(self):
        """ selectinquery() + filter """
        engine, ssn = self.engine, self.ssn

        # Test: load a relationship, filtered
        with QueryLogger(engine) as ql:
            q = ssn.query(models.User).options(selectinquery(
                models.User.articles,
                lambda q, **kw: q.filter(models.Article.id.between(11,21))
            ))
            res = q.all()

            # Test query
            if SA_12:
                # SqlAlchemy 1.2.x used to make a JOIN
                self.assertQuery(ql[1],
                                 'FROM u AS u_1 JOIN a ON u_1.id = a.uid',
                                 'WHERE u_1.id IN (1, 2, 3) AND '
                                 'a.id BETWEEN 11 AND 21 '
                                 'ORDER BY u_1.id',
                                 )
            else:
                # SqlAlchemy 1.3.x uses foreign keys directly, no joins
                self.assertNotIn(ql[1], 'JOIN')
                self.assertQuery(ql[1],
                                 'WHERE a.uid IN (1, 2, 3) AND ',
                                 'a.id BETWEEN 11 AND 21 ',
                                 # v1.3.16: no ordering by PK anymore
                                 'ORDER BY a.uid' if SA_UNTIL('1.3.15') else '',
                                 )


            # Test results
            self.assert_users_articles_comments(res, 3, 4, None)  # 3 users, 4 articles in total

    def test_plain_old_selectinload(self):
        """ Test plain selectinload() """
        engine, ssn = self.engine, self.ssn

        with QueryLogger(self.engine) as ql:
            q = ssn.query(models.User).options(selectinload(models.User.articles))
            res = q.all()

            # Test query
            if SA_12:
                self.assertQuery(ql[1],
                                 'WHERE u_1.id IN (1, 2, 3)',
                                 # v1.3.16: no ordering by PK anymore
                                 'ORDER BY u_1.id' if SA_UNTIL('1.3.15') else '',
                                 )
            else:
                self.assertQuery(ql[1],
                                 'WHERE a.uid IN (1, 2, 3)',
                                 # v1.3.16: no ordering by PK anymore
                                 'ORDER BY a.uid' if SA_UNTIL('1.3.15') else '',
                                 )

            # Test results
            self.assert_users_articles_comments(res, 3, 6, None)  # 3 users, 6 articles in total


    def test_options(self):
        """ selectinquery() + options(load_only()) + limit """
        engine, ssn = self.engine, self.ssn

        with QueryLogger(engine) as ql:
            q = ssn.query(models.User).options(selectinquery(
                models.User.articles,
                # Notice how we still have to apply the options using the relationship!
                lambda q, **kw: q.options(defaultload(models.User.articles)
                                          .load_only(models.Article.title)).limit(1)
            ))

            res = q.all()

            # Test query
            self.assertQuery(ql[1], 'LIMIT 1')
            if SA_12:
                self.assertSelectedColumns(ql[1], 'a.id', 'u_1.id', 'a.title')  # PK, FK, load_only()
            else:
                self.assertSelectedColumns(ql[1], 'a.id', 'a.uid', 'a.title')  # PK, FK, load_only()

            # Test results
            self.assert_users_articles_comments(res, 3, 1, None)  # 3 users, 1 article in total ; just one, because of the limit

    def test_options_joinedload(self):
        """ selectinquery() + options(joinedload()) """
        engine, ssn = self.engine, self.ssn

        with QueryLogger(engine) as ql:
            q = ssn.query(models.User).options(selectinquery(
                models.User.articles,
                lambda q, **kw: q.options(defaultload(models.User.articles)
                                          .joinedload(models.Article.comments))
            ))

            res = q.all()

            # Test query
            self.assertQuery(ql[1], 'LEFT OUTER JOIN c AS c_1 ON a.id = c_1.aid')

            # Test results
            self.assert_users_articles_comments(res, 3, 6, 9)  # 3 users, 6 articles, 9 comments

    def test_options_selectinload(self):
        """ selectinquery() + options(selectinload()) """
        engine, ssn = self.engine, self.ssn

        with QueryLogger(engine) as ql:
            q = ssn.query(models.User).options(selectinquery(
                models.User.articles,
                lambda q, **kw: q.options(defaultload(models.User.articles)
                                          .selectinload(models.Article.comments))
            ))

            res = q.all()

            # Test second query
            if SA_12:
                self.assertQuery(ql[2], 'JOIN c')
            else:
                self.assertQuery(ql[2], 'FROM c')

            # Test results
            self.assert_users_articles_comments(res, 3, 6, 9)  # 3 users, 6 articles, 9 comments

    def test_options_selectinquery(self):
        """ selectinquery() + load_only() + options(selectinquery() + load_only()) """
        engine, ssn = self.engine, self.ssn

        with QueryLogger(engine) as ql:
            q = ssn.query(models.User).options(selectinquery(
                models.User.articles,
                lambda q, **kw: q
                    .filter(models.Article.id > 10)  # first level filter()
                    .options(defaultload(models.User.articles)
                             .load_only(models.Article.title)  # first level options()
                             .selectinquery(models.Article.comments,
                                            lambda q, **kw:
                                            q
                                            .filter(models.Comment.uid > 1)  # second level filter()
                                            .options(
                                                defaultload(models.User.articles)
                                                    .defaultload(models.Article.comments)
                                                    .load_only(models.Comment.text)  # second level options()
                                            )))
            ))

            res = q.all()

            # Test query
            self.assertQuery(ql[1], 'AND a.id > 10')

            if SA_12:
                self.assertSelectedColumns(ql[1], 'a.id', 'u_1.id', 'a.title')  # PK, FK, load_only()
            else:
                self.assertSelectedColumns(ql[1], 'a.id', 'a.uid', 'a.title')  # PK, FK, load_only()

            # Test second query
            self.assertQuery(ql[2], 'AND c.uid > 1')

            if SA_12:
                self.assertSelectedColumns(ql[2], 'c.id', 'a_1.id', 'c.text')  # PK, FK, load_only()
            else:
                self.assertSelectedColumns(ql[2], 'c.id', 'c.aid', 'c.text')  # PK, FK, load_only()

            # Test results
            self.assert_users_articles_comments(res, 3, 5, 1)  # 3 users, 5 articles, 1 comment

    # Re-run all tests in wild combinations
    def test_all_tests_interference(self):
        """ Repeat all tests by randomly mixing them and running them in different order
            to make sure that they do not interfere with each other """
        all_tests = (getattr(self, name)
                     for name in dir(self)
                     if name.startswith('test_')
                     and name != 'test_all_tests_interference')

        for i in range(20):
            # Make a randomized mix of all tests
            tests = list(all_tests)
            shuffle(tests)

            # Run them all
            print('='*20 + ' Random run #{}'.format(i))
            for t in tests:
                try:
                    # Repeat every test several times
                    for n in range(3):
                        t()
                except unittest.SkipTest: pass  # proceed

    def assert_users_articles_comments(self, users, n_users, n_articles=None, n_comments=None):
        self.assertEqual(len(users), n_users)
        if n_articles is not None:
            self.assertEqual(sum(len(u.articles) for u in users), n_articles)
        if n_comments is not None:
            self.assertEqual(sum(sum(len(a.comments) for a in u.articles) for u in users), n_comments)
