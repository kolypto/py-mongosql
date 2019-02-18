import unittest

from sqlalchemy.orm import aliased

from . import models

from mongosql import ModelPropertyBags
from mongosql.bag import *


class BagsTest(unittest.TestCase):
    """ Test statements as strings """

    def test_user_bags(self):
        bags = ModelPropertyBags.for_model(models.User)

        self.assertEqual(bags.model, models.User)
        self.assertEqual(bags.model_name, 'User')

        #=== columns
        bag = bags.columns

        self.assertEqual(bag.names, {'id', 'name', 'tags', 'age'})
        self.assertEqual(len(list(bag)), 4)

        self.assertTrue('id' in bag)
        self.assertFalse('user_calculated' in bag)
        self.assertFalse('roles' in bag)

        self.assertIs(bag['id'], models.User.id)
        self.assertRaises(KeyError, bag.__getitem__, 'NOPE')

        self.assertTrue(bag.is_column_array('tags'))
        self.assertFalse(bag.is_column_array('id'))

        self.assertEqual(bag.get_invalid_names(['id', 'name', 'NOPE']), {'NOPE'})

        #=== columns. dot-notation
        pass

        #=== relations
        bag = bags.relations

        self.assertEqual(bag.names, {'roles', 'comments', 'articles'})

        self.assertTrue('roles' in bag)
        self.assertFalse('id' in bag)

        self.assertIs(bag['roles'], models.User.roles)
        self.assertRaises(KeyError, bag.__getitem__, 'NOPE')

        self.assertTrue(bag.is_relationship_array('roles'))

        self.assertEqual(bag.get_invalid_names(['roles', 'NOPE']), {'NOPE'})

        #=== pk, nullable, properties, hybrid properties
        self.assertEqual(bags.pk.names, {'id'})
        self.assertEqual(bags.nullable.names, {'name', 'tags', 'age'})
        self.assertEqual(bags.properties.names, {'user_calculated'})
        self.assertEqual(bags.hybrid_properties.names, set())

        #=== rel_columns. Dot-notation
        bag = bags.related_columns

        self.assertGreaterEqual(bag.names, {'roles.id', 'comments.id', 'articles.title'})  # just a few

        self.assertIn('roles.id', bag)
        self.assertIn('articles.title', bag)
        self.assertNotIn('roles', bag)

        self.assertEqual(bag.get_invalid_names(['roles', 'roles.id', 'NOPE']), {'roles', 'NOPE'})


        #== combined: rel_columns + columns + hybrid
        cbag = CombinedBag(
            col=bags.columns,
            rcol=bags.related_columns,
            hybrid=bags.hybrid_properties
        )

        self.assertGreaterEqual(cbag.names, {'id', 'roles.id'})

        self.assertIn('id', cbag)
        self.assertIn('roles.id', cbag)
        self.assertNotIn('roles', cbag)

        self.assertEqual(len(list(cbag)), 17)  # all properties properly iterated over

        bag_name, bag, col = cbag['id']
        self.assertEqual(bag_name, 'col')
        self.assertIs(col, models.User.id)
        self.assertEqual(type(bag), DotColumnsBag)
        self.assertFalse(bag.is_column_array('id'))

        bag_name, bag, col = cbag['tags']
        self.assertEqual(bag_name, 'col')
        self.assertIs(col, models.User.tags)
        self.assertEqual(type(bag), DotColumnsBag)
        self.assertTrue(bag.is_column_array('tags'))

        bag_name, bag, col = cbag['articles.id']
        self.assertEqual(bag_name, 'rcol')
        self.assertIs(col, models.Article.id)
        self.assertEqual(type(bag), DotRelatedColumnsBag)
        self.assertFalse(bag.is_column_array('id'))
        self.assertTrue(bag.is_relationship_array('articles'))

    def test_article_bags(self):
        bags = ModelPropertyBags.for_model(models.Article)

        self.assertEqual(bags.model, models.Article)
        self.assertEqual(bags.model_name, 'Article')

        #=== columns
        # Pay attention to the `hybrid` and `data` attributes: hybrid, and JSON
        bag = bags.columns

        self.assertEqual(bag.names, {'id', 'uid', 'title', 'theme', 'data'})

        self.assertFalse('hybrid' in bag)
        self.assertFalse('user' in bag)

        self.assertIs(bag['id'], models.Article.id)

        self.assertFalse(bag.is_column_json('id'))
        self.assertTrue(bag.is_column_json('data'))

        # === columns. dot-notation
        self.assertIs(bag['data'], models.Article.data)
        self.assertTrue('data.id' in bag)
        self.assertEqual(str(bag['data.id']), 'a.data #>> :data_1')  # SQL expression
        self.assertRaises(KeyError, bag.__getitem__, 'title.id')  # not JSON

        # All fields validated ok
        self.assertEqual(bag.get_invalid_names(['id', 'data', 'data.rating']), set())  # JSON prop
        self.assertEqual(bag.get_invalid_names(['id.rating']), {'id.rating'})  # not JSON

        # === relations
        bag = bags.relations

        self.assertEqual(bag.names, {'user', 'comments'})

        self.assertTrue('user' in bag)

        self.assertIs(bag['user'], models.Article.user)
        self.assertRaises(KeyError, bag.__getitem__, 'NOPE')

        self.assertFalse(bag.is_relationship_array('user'))
        self.assertTrue(bag.is_relationship_array('comments'))

        self.assertEqual(bag.get_invalid_names(['user', 'NOPE']), {'NOPE'})

        # === pk, nullable, properties, hybrid properties
        self.assertEqual(bags.pk.names, {'id'})
        self.assertEqual(bags.nullable.names, {'uid', 'title', 'theme', 'data'})
        self.assertEqual(bags.properties.names, {'calculated'})
        self.assertEqual(bags.hybrid_properties.names, {'hybrid'})

        # === rel_columns. Dot-notation
        bag = bags.related_columns

        self.assertGreaterEqual(bag.names, {'user.id', 'comments.id', 'comments.text'})  # just a few
        self.assertNotIn('roles', bag)

        self.assertIn('user.id', bag)
        self.assertIn('comments.text', bag)
        self.assertNotIn('user', bag)

        self.assertEqual(bag.get_invalid_names(['user', 'user.id', 'NOPE']), {'user', 'NOPE'})

        self.assertFalse(bag.is_column_array('user.id'))
        self.assertTrue(bag.is_column_array('user.tags'))

        self.assertFalse(bag.is_relationship_array('user'))
        self.assertTrue(bag.is_relationship_array('comments'))

        self.assertIs(bag.get_relationship('user.id'), models.Article.user)

        # === Combined: columns + hybrid
        cbag = CombinedBag(
            col=bags.columns,
            hybrid=bags.hybrid_properties
        )

        bag_name, bag, col = cbag['data.id']  # cbag transparently lets you access JSON column attrs
        self.assertEqual(bag_name, 'col')
        self.assertEqual(str(col), 'a.data #>> :data_1')  # SQL expression
        self.assertTrue('data.id' in cbag)

    def test_aliased_article_bags(self):
        # This is not really important, but our bags seem to ignore Python properties of an
        # aliased class. This is not right... :)
        bags = ModelPropertyBags(aliased(models.Article))

        self.assertIn('id', bags.columns)
        self.assertIn('user', bags.relations)
        self.assertIn('user.id', bags.related_columns)
        self.assertIn('calculated', bags.properties)
        self.assertIn('hybrid', bags.hybrid_properties)
