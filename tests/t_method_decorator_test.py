import unittest
from functools import wraps

from mongosql.util.method_decorator import method_decorator


class method_decorator_test(unittest.TestCase):
    def test_method_decorators(self):
        # === Test: method decorator
        # Create a class
        class A:
            @method_decorator_1(1)
            def a(self): pass

            @method_decorator_1(2)
            def b(self): pass

            @method_decorator_2(3)
            def c(self): pass

        # isinstance() checks
        self.assertTrue(isinstance(method_decorator.get_method_decorator(A, 'a'), method_decorator_1))
        self.assertTrue(isinstance(method_decorator.get_method_decorator(A, 'b'), method_decorator_1))
        self.assertTrue(isinstance(method_decorator.get_method_decorator(A, 'c'), method_decorator_2))

        self.assertFalse(isinstance(method_decorator.get_method_decorator(A, 'a'), method_decorator_2))
        self.assertFalse(isinstance(method_decorator.get_method_decorator(A, 'b'), method_decorator_2))
        self.assertFalse(isinstance(method_decorator.get_method_decorator(A, 'c'), method_decorator_1))

        # Collect: decorator 1
        m1s = method_decorator_1.all_decorators_from(A)
        self.assertEqual(len(m1s), 2)
        self.assertEqual(m1s[0].method_name, 'a')
        self.assertEqual(m1s[0].arg1, 1)
        self.assertEqual(m1s[1].method_name, 'b')
        self.assertEqual(m1s[1].arg1, 2)

        # Collect: decorator 2
        m2s = method_decorator_2.all_decorators_from(A)
        self.assertEqual(len(m2s), 1)
        self.assertEqual(m2s[0].method_name, 'c')
        self.assertEqual(m2s[0].arg2, 3)

        # === Test: now try to mix it with other decorators
        class B:
            @nop_decorator  # won't hide it
            @method_decorator_1(0)
            def a(self): pass

            @method_decorator_1(0)
            @nop_decorator
            def b(self): pass

        # isinstance() checks
        # They work even through the second decorator
        self.assertTrue(isinstance(method_decorator.get_method_decorator(B, 'a'), method_decorator_1))
        self.assertTrue(isinstance(method_decorator.get_method_decorator(B, 'b'), method_decorator_1))

        # Collect
        m1s = method_decorator_1.all_decorators_from(B)
        self.assertEqual(len(m1s), 2)
        self.assertEqual(m1s[0].method_name, 'a')
        self.assertEqual(m1s[1].method_name, 'b')

        # === Test: apply two method_decorators at the same time!
        class C:
            @nop_decorator
            @method_decorator_1(1)
            @method_decorator_2(2)
            def a(self): pass

            @nop_decorator
            @method_decorator_2(3)
            @method_decorator_1(4)
            def b(self): pass

        # isinstance() checks
        self.assertTrue(isinstance(method_decorator.get_method_decorator(C, 'a'), method_decorator_1))
        self.assertTrue(isinstance(method_decorator.get_method_decorator(C, 'a'), method_decorator_2))
        self.assertTrue(isinstance(method_decorator.get_method_decorator(C, 'b'), method_decorator_1))
        self.assertTrue(isinstance(method_decorator.get_method_decorator(C, 'b'), method_decorator_2))

        # Collect: decorator 1
        m1s = method_decorator_1.all_decorators_from(C)
        self.assertEqual(len(m1s), 2)
        self.assertEqual(m1s[0].method_name, 'a')
        self.assertEqual(m1s[0].arg1, 1)
        self.assertEqual(m1s[1].method_name, 'b')
        self.assertEqual(m1s[1].arg1, 4)

        # Collect: decorator 2
        m2s = method_decorator_2.all_decorators_from(C)
        self.assertEqual(len(m2s), 2)
        self.assertEqual(m2s[0].method_name, 'a')
        self.assertEqual(m2s[0].arg2, 2)
        self.assertEqual(m2s[1].method_name, 'b')
        self.assertEqual(m2s[1].arg2, 3)



# Example wrappers

class method_decorator_1(method_decorator):
    def __init__(self, arg):
        super().__init__()
        self.arg1 = arg


class method_decorator_2(method_decorator):
    def __init__(self, arg):
        super().__init__()
        self.arg2 = arg


def nop_decorator(f):
    @wraps(f)
    def wrapper(*a, **k):
        return f(*a, **k)
    return wrapper
