import unittest
from sqlalchemy.orm import load_only, lazyload

from . import models
from .util import ExpectedQueryCounter
from mongosql.util.history_proxy import ModelHistoryProxy


class HistoryTest(unittest.TestCase):
    """ Test MongoQuery """

    @classmethod
    def setUpClass(cls):
        # Init db
        cls.engine, cls.Session = models.get_working_db_for_tests(autoflush=False)

    def test_model_history__loses_history_on_flush(self):
        # Session is reused
        ssn = self.Session()
        assert ssn.autoflush is False, 'these tests rely on Session not having an autoflush'

        # === Test 1: ModelHistoryProxy does not lose history when lazyloading a column
        user = ssn.query(models.User).options(
            load_only('name'),
        ).get(1)

        with ExpectedQueryCounter(self.engine, 0, 'Expected no queries here'):
            # Prepare a ModelHistoryProxy
            old_user_hist = ModelHistoryProxy(user)

            # Modify
            user.name = 'CHANGED'

            # History works
            self.assertEqual(old_user_hist.name, 'a')

        # Load a column
        with ExpectedQueryCounter(self.engine, 1, 'Expected 1 lazyload query'):
            user.age

        # History is NOT broken!
        self.assertEqual(old_user_hist.name, 'a')

        # Change another column; history is NOT broken!
        user.age = 1800
        self.assertEqual(old_user_hist.age, 18)



        # === Test 2: ModelHistoryProxy does not lose history when lazyloading a one-to-many relationship
        user = ssn.query(models.User).get(1)

        with ExpectedQueryCounter(self.engine, 0, 'Expected no queries here'):
            # Prepare a ModelHistoryProxy
            old_user_hist = ModelHistoryProxy(user)

            # Modify
            user.name = 'CHANGED'

            # History works
            self.assertEqual(old_user_hist.name, 'a')

        # Load a relationship
        with ExpectedQueryCounter(self.engine, 1, 'Expected 1 lazyload query'):
            list(user.articles)

        # History is NOT broken!
        self.assertEqual(old_user_hist.name, 'a')



        # === Test 3: ModelHistoryProxy does not lose history when lazyloading a one-to-one
        # We intentionally choose an article by another user (uid=2),
        # because User(uid=1) is cached in the session, and accessing `article.user` would just reuse it.
        # We want a new query, however
        article = ssn.query(models.Article).get(20)

        with ExpectedQueryCounter(self.engine, 0, 'Expected no queries here'):
            # Prepare a ModelHistoryProxy
            old_article_hist = ModelHistoryProxy(article)

            # Modify
            article.title = 'CHANGED'

            # History works
            self.assertEqual(old_article_hist.title, '20')

        # Load a relationship
        with ExpectedQueryCounter(self.engine, 1, 'Expected 1 lazyload query'):
            article.user

        # History is NOT broken!
        self.assertEqual(old_article_hist.title, '20')



        # === Test 4: ModelHistoryProxy does not lose history when flushing a session
        user = ssn.query(models.User).options(
            load_only('name'),
        ).get(1)
        with ExpectedQueryCounter(self.engine, 0, 'Expected no queries here'):
            # Prepare a ModelHistoryProxy
            old_user_hist = ModelHistoryProxy(user)

            # Modify
            user.name = 'CHANGED'

            # History works
            self.assertEqual(old_user_hist.name, 'a')

        # Flush session
        ssn.flush()

        # History is NOT broken
        self.assertEqual(old_user_hist.name, 'a')

        # Undo
        ssn.rollback()  # undo our changes
        ssn.close()  # have to close(), or other queries might hang


    def test_model_history__both_classes(self):
        ssn = self.Session()
        # Get a user from the DB
        user = ssn.query(models.User).options(
            lazyload('*')
        ).get(1)

        # Prepare two history objects
        old_user = ModelHistoryProxy(user)

        # Check `user` properties
        self.assertEqual(user.id, 1)
        self.assertEqual(user.name, 'a')
        self.assertEqual(user.age, 18)
        self.assertEqual(user.tags, ['1', 'a'])

        # === Test: columns
        # Check `old_user` properties
        # self.assertEqual(old_user.id, 1)
        self.assertEqual(old_user.name, 'a')
        self.assertEqual(old_user.age, 18)
        self.assertEqual(old_user.tags, ['1', 'a'])

        # Change `user`
        user.id = 1000
        user.name = 'aaaa'
        user.age = 1800
        user.tags = [1000,]

        # Check `old_user` retains properties
        self.assertEqual(old_user.id, 1)
        self.assertEqual(old_user.name, 'a')
        self.assertEqual(old_user.age, 18)
        self.assertEqual(old_user.tags, ['1', 'a'])

        # Undo
        ssn.close()

    # Older tests

    def test_change_field(self):
        ssn = self.Session()
        comment = ssn.query(models.Comment).get(100)
        old_text = comment.text
        comment.text = 'Changed two'
        hist = ModelHistoryProxy(comment)
        # When you load a relationship, model history is dropped
        # This happens because History is reset on flush(), which happens with a query
        user = comment.user
        self.assertEqual(hist.text, old_text)
        ssn.close()

        # Test for json fields
        ssn = self.Session()
        article = ssn.query(models.Article).get(10)
        old_rating = article.data['rating']
        hist = ModelHistoryProxy(article)
        article.data['rating'] = 11111

        self.assertEqual(hist.data['rating'], old_rating)
        article.data = {'one': {'two': 2}}
        ssn.add(article)
        ssn.flush()
        article = ssn.query(models.Article).get(10)
        hist = ModelHistoryProxy(article)
        article.data['one']['two'] = 10
        self.assertEqual(hist.data['one']['two'], 2)

        # Undo
        ssn.rollback()
        ssn.close()

    def test_model_property(self):
        ssn = self.Session()

        # Get one comment
        comment = ssn.query(models.Comment).get(100)

        # Check the original value
        old_value = '0-a'
        self.assertEqual(comment.comment_calc, old_value)

        # Change the value of another attribute: the one @property depends on
        comment.text = 'Changed one'

        # Try to build history after the fact
        hist = ModelHistoryProxy(comment)

        # Historical value for a @property
        self.assertEqual(hist.comment_calc, old_value)

        # Current value
        self.assertEqual(comment.comment_calc, 'one')

        # Undo
        ssn.close()

    def test_relation(self):
        ssn = self.Session()
        comment = ssn.query(models.Comment).get(100)

        old_id = comment.user.id
        hist = ModelHistoryProxy(comment)
        new_user = ssn.query(models.User).filter(models.User.id != old_id).first()
        comment.user = new_user
        article = comment.article  # load a relationship; see that history is not reset
        self.assertEqual(hist.user.id, old_id)  # look how we can access relationship's attrs through history!

        article = ssn.query(models.Article).get(10)

        old_commensts = set([c.id for c in article.comments])
        article.comments = article.comments[:1]
        hist = ModelHistoryProxy(article)
        u = article.user  # load a relationship; see that history is not reset
        self.assertEqual(old_commensts, set([c.id for c in hist.comments]))

        # Undo
        ssn.close()
