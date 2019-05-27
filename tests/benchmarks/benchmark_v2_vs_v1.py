from tests.benchmarks.benchmark_utils import benchmark_parallel_funcs
from mongosql.handlers import MongoJoin
from tests.models import *

# Import both MongoSQL packages
try:
    from tests.benchmarks.mongosql_v1.mongosql import MongoQuery as MongoQuery_v1
except ImportError:
    print('Please install MongoSQL 1.5: ')
    print('$ bash tests/benchmarks/mongosql_v1_checkout.sh')
    exit(1)

from mongosql import MongoQuery as MongoQuery_v2

# Check SqlAlchemy version
from sqlalchemy import __version__ as SA_VERSION
assert SA_VERSION.startswith('1.2.'), 'Only works with SqlAlchemy 1.2.x'



# Init DB: choose one
engine, Session = get_working_db_for_tests()

# Prepare
N_REPEATS = 1000
MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = False
ssn = Session()

# Tests
def test_v1(n):
    """ Test MongoSQL v1 """
    for i in range(n):
        q = MongoQuery_v1(User, query=ssn.query(User)).query(
            project=['name'],
            filter={'age': {'$ne': 100}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': {'$ne': 'sci-fi'}},
                                   join={'comments': dict(project=['aid'],
                                                          filter={'text': {'$exists': True}})})}
        ).end()
        list(q.all())

def test_v2(n):
    """ Test MongoSQL v2 """
    for i in range(n):
        q = MongoQuery_v2(User).with_session(ssn).query(
            project=['name'],
            filter={'age': {'$ne': 100}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': {'$ne': 'sci-fi'}},
                                   join={'comments': dict(project=['aid'],
                                                          filter={'text': {'$exists': True}})})}
        ).end()
        list(q.all())

# Run
print('Running tests...')
res = benchmark_parallel_funcs(
    N_REPEATS, 10,
    test_v1,
    test_v2
)

# Done
print(res)
