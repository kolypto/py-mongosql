import unittest

from . import models
from mongosql.hist import ModelHistoryProxy


class HistoryTest(unittest.TestCase):
    """ Test MongoQuery """

    def setUp(self):
        # Connect, create tables
        engine, Session = models.init_database(autoflush=False)
        models.drop_all(engine)
        models.create_all(engine)

        # Fill DB
        ssn = Session()
        ssn.add_all(models.content_samples())
        ssn.commit()

        # Session
        self.Session = Session
        self.engine = engine
        self.db = Session()

    def tearDown(self):
        self.db.close()  # Need to close the session: otherwise, drop_all() hangs forever
        models.drop_all(self.engine)

    def test_change_field(self):
        comment = self.db.query(models.Comment).first()
        old_text = comment.text
        comment.text = 'Changed text'
        hist = ModelHistoryProxy(comment)
        # When you load a relationship, model history is dropped
        # This happens because History is reset on flush(), which happens with a query
        user = comment.user
        self.assertEqual(hist.text, old_text)

        # Test for json fields
        article = self.db.query(models.Article).get(10)
        old_rating = article.data['rating']
        hist = ModelHistoryProxy(article)
        article.data['rating'] = 11111

        self.assertEqual(hist.data['rating'], old_rating)
        article.data = {'one': {'two': 2}}
        self.db.add(article)
        self.db.commit()
        article = self.db.query(models.Article).get(10)
        hist = ModelHistoryProxy(article)
        article.data['one']['two'] = 10
        self.assertEqual(hist.data['one']['two'], 2)

    def test_model_property(self):
        comment = self.db.query(models.Comment).first()
        old_prop = comment.comment_calc
        comment.text = 'Changed text'
        hist = ModelHistoryProxy(comment)
        self.assertEqual(hist.comment_calc, old_prop)
        self.assertNotEqual(hist.comment_calc, comment.comment_calc)

    def test_relation(self):
        comment = self.db.query(models.Comment).first()

        old_id = comment.user.id
        hist = ModelHistoryProxy(comment)
        new_user = self.db.query(models.User).filter(models.User.id != old_id).first()
        comment.user = new_user
        article = comment.article  # load a relationship; see that history is not reset
        self.assertEqual(hist.user.id, old_id)  # look how we can access relationship's attrs through history!

        article = self.db.query(models.Article).get(10)

        old_commensts = set([c.id for c in article.comments])
        article.comments = article.comments[:1]
        hist = ModelHistoryProxy(article)
        u = article.user  # load a relationship; see that history is not reset
        self.assertEqual(old_commensts, set([c.id for c in hist.comments]))
