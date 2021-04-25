import unittest

from sqlalchemy.orm import aliased

from . import models

from mongosql.bag import *
from .saversion import SA_12, SA_13, SA_14, SA_SINCE, SA_UNTIL

class BagsTest(unittest.TestCase):
    """ Test bags """

    maxDiff = None

    def test_user_bags(self):
        bags = ModelPropertyBags.for_model(models.User)

        self.assertEqual(bags.model, models.User)
        self.assertEqual(bags.model_name, 'User')

        #=== columns
        bag = bags.columns

        self.assertEqual(bag.names, {'id', 'name', 'tags', 'age', 'master_id'})
        self.assertEqual(len(list(bag)), 5)

        self.assertTrue('id' in bag)
        self.assertFalse('user_calculated' in bag)
        self.assertFalse('roles' in bag)

        self.assertIs(bag['id'], models.User.id)
        self.assertRaises(KeyError, bag.__getitem__, 'NOPE')

        self.assertTrue(bag.is_column_array('tags'))
        self.assertFalse(bag.is_column_array('id'))

        self.assertEqual(bag.get_invalid_names(['id', 'name', 'NOPE']), {'NOPE'})

        self.assertEqual(sorted(list(bag)), [
            ('age', models.User.age),
            ('id', models.User.id),
            ('master_id', models.User.master_id),
            ('name', models.User.name),
            ('tags', models.User.tags),
        ])

        #=== columns. dot-notation
        pass

        #=== properties.
        pass

        #=== hybrid_properties
        pass

        # === association_proxies
        bag = bags.association_proxies
        self.assertEqual(bag.names, set())

        #=== relations
        bag = bags.relations

        self.assertEqual(bag.names, {'roles', 'comments', 'articles', 'master'})

        self.assertTrue('roles' in bag)
        self.assertFalse('id' in bag)

        self.assertIs(bag['roles'], models.User.roles)
        self.assertRaises(KeyError, bag.__getitem__, 'NOPE')

        self.assertTrue(bag.is_relationship_array('roles'))

        self.assertEqual(bag.get_invalid_names(['roles', 'NOPE']), {'NOPE'})

        self.assertEqual(set(dict(bag)), {'roles', 'comments', 'articles', 'master'})

        #=== pk, nullable, properties, hybrid properties
        self.assertEqual(bags.pk.names, {'id'})
        self.assertEqual(bags.nullable.names, {'name', 'tags', 'age', 'master_id'})
        self.assertEqual(bags.properties.names, {'user_calculated'})
        self.assertEqual(bags.hybrid_properties.names, set())

        #=== related_columns. Dot-notation
        bag = bags.related_columns

        self.assertGreaterEqual(bag.names, {'roles.id', 'comments.id', 'articles.title'})  # just a few

        self.assertIn('roles.id', bag)
        self.assertIn('articles.title', bag)
        self.assertNotIn('roles', bag)

        self.assertEqual(bag.get_invalid_names(['roles', 'roles.id', 'NOPE']), {'roles', 'NOPE'})

        self.assertGreaterEqual(set(dict(bag)), {'comments.uid', 'articles.id', 'articles.data'})


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

        self.assertEqual(len(list(cbag)), 23)  # all properties properly iterated over

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

        # Iteration: tuples of 4
        listed_bag = sorted(list(cbag))
        bag_name, bag, col_name, col = listed_bag[0]
        self.assertEqual(bag_name, 'col')
        self.assertEqual(bag, cbag.bag('col'))
        self.assertEqual(col_name, 'age')
        self.assertIs(col, models.User.age)

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

        # === association_proxies
        bag = bags.association_proxies
        self.assertEqual(bag.names, set())

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

        # === related_columns. Dot-notation
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
        # Make sure that all Bags can work with aliased classes
        a = models.Article
        aa = aliased(models.Article, name='a_1')

        # ModelPropertyBags() does not tolerate aliases
        with self.assertRaises(TypeError):
            ModelPropertyBags(aliased(models.Article))

        # Init bags
        bags = ModelPropertyBags.for_alias(aa)

        # Test that we're using a lazy wrapper
        self.assertNotIsInstance(bags, ModelPropertyBags)

        # Test that every bag's dicts got wrapped
        # Yes, we're testing their protected properties
        self.assertIsInstance(bags.columns._columns, DictOfAliasedColumns)
        # self.assertIsInstance(bags.hybrid_properties._columns, DictOfAliasedColumns)  # TODO: It fails. See: HybridPropertiesBag.aliased
        self.assertIsInstance(bags.pk._columns, DictOfAliasedColumns)
        self.assertIsInstance(bags.nullable._columns, DictOfAliasedColumns)


        # Test every bag and make sure it returns columns of an aliased model, not the original model
        # To test this, we're compiling its `Column.expression` to string: it should refer to the aliased table.

        def test_column(column, expected_column_expression):
            # Test whether the given column renders an expression that references the aliased model correctly
            self.assertEqual(str(column.expression), expected_column_expression)

        def get_column_by_iter(bag, col_name):
            # Get a column through __iter__() -- for testing __iter__()
            return dict(bag.__iter__())[col_name]  # __iter__() -> dict() -> get item

        def test_column_by_get(bag, col_name, expected_column_expression):
            # Test __getitem__()
            test_column(bag[col_name], expected_column_expression)

        def test_column_by_iter(bag, col_name, expected_column_expression):
            # Test __iter__()
            test_column(get_column_by_iter(bag, col_name), expected_column_expression)

        def test_column_by_get_and_iter(bag, col_name, expected_column_expression):
            # Test __getitem__() and __iter__()
            test_column_by_get(bag, col_name, expected_column_expression)
            test_column_by_iter(bag, col_name, expected_column_expression)

        # === Test: Sanity check: the expected behavior
        self.assertEqual(str(aa.id), 'AliasedClass_Article.id')
        self.assertEqual(str(aa.data), 'AliasedClass_Article.data')
        self.assertEqual(str(aa.hybrid), 'AliasedClass_Article.hybrid')
        self.assertEqual(str(aa.id.expression), 'a_1.id')
        self.assertEqual(str(aa.data.expression), 'a_1.data')
        self.assertIn('a_1', str(aa.hybrid.expression))

        self.assertEqual(str(a.id.adapt_to_entity(inspect(aa))), 'AliasedClass_Article.id')
        self.assertEqual(str(a.data.adapt_to_entity(inspect(aa))), 'AliasedClass_Article.data')
        self.assertEqual(str(a.hybrid.adapt_to_entity(inspect(aa))), 'AliasedClass_Article.hybrid')
        self.assertEqual(str(a.id.adapt_to_entity(inspect(aa)).expression), 'a_1.id')
        self.assertEqual(str(a.data.adapt_to_entity(inspect(aa)).expression), 'a_1.data')
        # self.assertIn('a_1', str(a.hybrid.adapt_to_entity(inspect(aa)).expression))  # TODO: It fails. See: HybridPropertiesBag.aliased

        # === Test: bags.columns: DotColumnsBag
        bag = bags.columns

        # Test ordinary column
        test_column_by_get_and_iter(bag, 'id', 'a_1.id')

        # Test JSON column
        test_column_by_get(bag, 'data.rating', 'a_1.data #>> :data_1')  # correct even for

        # Test get_column()
        self.assertEqual(str(bag.get_column('data.rating')), 'AliasedClass_Article.data')

        # === Test: bags.properties: PropertiesBag
        # ... columns are never returned, because they do not exist in SQL :)
        # Nothing to test here

        # === Test: bags.hybrid_properties: HybridPropertiesBag(ColumnsBag)
        bag = bags.hybrid_properties

        c = bag['hybrid']
        self.assertEqual(str(c), 'AliasedClass_Article.hybrid')
        expr = str(c.expression)
        self.assertIn('a_1.id > :id_1', expr)
        self.assertIn('FROM u, a AS a_1', expr)
        self.assertIn('u.id = a_1.uid', expr)

        # === Test: bags.relations: RelationshipsBag
        bag = bags.relations

        self.assertEqual(str(bag['user']), 'AliasedClass_Article.user')
        self.assertEqual(str(bag['comments']), 'AliasedClass_Article.comments')
        test_column(bag['user'], 'u.id = a_1.uid')  # join condition uses aliases!
        test_column(bag['comments'], 'a_1.id = c.aid')

        self.assertIs(bag.get_target_model('user'), models.User)

        # === Test: bags.pk: PrimaryKeyBag(ColumnsBag)
        bag = bags.pk
        test_column_by_get_and_iter(bag, 'id', 'a_1.id')

        # === Test: bags.nullable: ColumnsBag
        bag = bags.nullable
        test_column_by_get_and_iter(bag, 'title', 'a_1.title')

        # === Test: bags.related_columns: DotRelatedColumnsBag
        bag = bags.related_columns

        test_column_by_get_and_iter(bag, 'user.id', 'u.id')

        # === Test: CombinedBag
        bag = CombinedBag(
            col=bags.columns,
            rcol=bags.related_columns,
            hybrid=bags.hybrid_properties,
        )

        bag_name, bag, c = bag['id']
        test_column(c, 'a_1.id')

        return

    def test_girl_watcher_bags(self):
        bags = ModelPropertyBags.for_model(models.GirlWatcher)
        ins = inspect(models.GirlWatcher)

        # === columns
        bag = bags.columns
        self.assertEqual(bag.names, {'id', 'name', 'age', 'favorite_id'})

        # === association_proxies
        bag = bags.association_proxies
        if SA_12:
            self.assertEqual(bag.names, set())  # ignored in SqlAlchemy 1.2.x
        else:
            self.assertEqual(bag.names, {'good_names', 'best_names'})

        # === relations
        bag = bags.relations
        self.assertEqual(bag.names, {'favorite', 'good', 'best', 'manager'})

        # === pk, nullable, properties, hybrid properties
        self.assertEqual(bags.pk.names, {'id'})
        self.assertEqual(bags.nullable.names, {'name', 'age', 'favorite_id'})
        self.assertEqual(bags.properties.names, set())
        self.assertEqual(bags.hybrid_properties.names, set())

        # === related_columns. Dot-notation
        bag = bags.related_columns

        self.assertGreaterEqual(bag.names, {'good.id', 'best.id'})  # just a few

    def test_writable_properties(self):
        """ Test how mongosql detects writable properties """
        bags = ModelPropertyBags.for_model(models.ManyPropertiesModel)

        # @property
        # `_p_invisible` is missing
        self.assertEqual(bags.properties.names, {'p_readonly', 'p_writable'})
        self.assertEqual(bags.writable_properties.names, {'p_writable'})

        # @hybrid_property
        self.assertEqual(bags.hybrid_properties.names, {'hp_readonly', 'hp_writable'})
        self.assertEqual(bags.writable_hybrid_properties.names, {'hp_writable'})

        # both, if writable
        self.assertEqual(bags.writable.names, {'id', 'p_writable', 'hp_writable'})



        # Test column_property(): should not be writable
        bags = ModelPropertyBags.for_model(models.Role)
        self.assertEqual(bags.writable_properties.names, frozenset())
        self.assertEqual(bags.writable_hybrid_properties.names, frozenset())
        self.assertEqual(bags.writable.names, {'id', 'uid', 'description', 'title'})  # "is_admin" not here

    def test_bag_is_reused(self):
        """ Test that ModelPropertyBags is reused every time """
        # Test that we get the same bag every time
        a = ModelPropertyBags.for_model(models.Article)
        b = ModelPropertyBags.for_model(models.Article)
        self.assertIs(a, b)

        # Test that when a bag is aliased, it is a different object
        aa = ModelPropertyBags.for_model(models.Article).aliased(aliased(models.User))
        self.assertIsNot(aa, a)

        # Test that after calling aliased(), for_model() still returns unadulterated bags
        self.assertIs(ModelPropertyBags.for_model(models.Article), a)

    def test_mixins_car_article(self):
        """ Test table mixins """
        # First, load Article
        # MongoSql used to install its custom property on the parent model, and the child model used to read it.
        # This is unacceptable: these two have to be two different bags!
        ModelPropertyBags.for_model(models.Article)
        # Only then, load CarArticle
        bags = ModelPropertyBags.for_model(models.CarArticle)

        # === columns
        bag = bags.columns
        self.assertSetEqual(bag.names, {'id', 'uid', 'title', 'theme', 'data',  # Article
                                        'id',  # CarArticle
                                        'cuid', 'ctime',  # mixin
                                        })

        # === relations
        bag = bags.relations
        self.assertSetEqual(bag.names, {'user', 'comments',  # Article
                                        'car',  # CarArticle
                                        'cuser',  # mixin
                                        })

        # === related columns
        bag = bags.related_columns
        self.assertGreaterEqual(bag.names, {'user.id', 'comments.id',  # Article
                                            'cuser.id',  # mixin
                                            'car.id',  # CarAticle
                                            })

        # === pk, nullable, properties, hybrid properties
        self.assertSetEqual(bags.pk.names, {'id'})
        self.assertSetEqual(bags.nullable.names, {'uid', 'title', 'theme', 'data',  # Article
                                                  'cuid', 'ctime',  # mixin
                                                  })
        self.assertSetEqual(bags.properties.names, {'calculated',  # Article
                                                    'get_42'  # mixin
                                                    })
        self.assertSetEqual(bags.hybrid_properties.names, {'hybrid',  # Article
                                                           'hyb_big_id'  # Mixin
                                                           })


    def test_inheritance_cars(self):
        """ Test table inheritance """
        bags = ModelPropertyBags.for_model(models.ElectricCar)

        # === columns
        bag = bags.columns
        self.assertSetEqual(bag.names, {'id', 'type', 'make', 'model', 'horses', 'article_id',  # Car
                                        'id', 'batt_capacity',  # ElectricCar
                                        })

        # === relations
        bag = bags.relations
        self.assertSetEqual(bag.names, {'article',  # Car
                                        # None for ElectricCar
                                        })

        # === related columns
        bag = bags.related_columns
        self.assertGreaterEqual(bag.names, {'article.id',  # Car
                                            # None for ElectricCar
                                            })

        # === pk, nullable, properties, hybrid properties
        self.assertSetEqual(bags.pk.names, {'id'})
        self.assertSetEqual(bags.nullable.names, {'type', 'make', 'model', 'horses', 'article_id',  # Car
                                                  'batt_capacity',  # ElectricCar
                                                  })
        self.assertSetEqual(bags.properties.names, set())
        self.assertSetEqual(bags.hybrid_properties.names, set())

    def test_special_cases(self):
        bags = ModelPropertyBags.for_model(models.CollectionOfSpecialCases)

        # Make sure that decorated column types (JSON, JSONB, ARRAY) are detected properly
        # It matters, because if a CrudHelper fails to detect a field as JSON, it will not do a shallow merge.
        # Here: mongosql.crud.crudhelper.CrudHelper._update_model
        bag = bags.columns
        self.assertTrue(bag.is_column_json('decorated_jsonb'))
        self.assertTrue(bag.is_column_json('decorated_mutable_jsonb'))
        self.assertTrue(bag.is_column_array('decorated_array'))
