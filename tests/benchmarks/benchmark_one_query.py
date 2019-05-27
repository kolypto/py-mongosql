from tests.benchmarks.benchmark_utils import benchmark_parallel_funcs

from mongosql.handlers import MongoJoin
from tests.models import *

# Run me:
# $ python -m cProfile -o profile.out tests/benchmark_one_query.py

# Init DB: choose one
# engine, Session = get_big_db_for_benchmarks(100, 10, 3)
engine, Session = get_working_db_for_tests()
# engine, Session = get_empty_db()

# Prepare
N_REPEATS = 1000
ssn = Session()

# Tests
def run_query(n):
    for i in range(n):
        q = User.mongoquery(ssn).query(
            project=['name'],
            filter={'age': {'$ne': 100}},
            join={'articles': dict(project=['title'],
                                   filter={'theme': {'$ne': 'sci-fi'}},
                                   join={'comments': dict(project=['aid'],
                                                          filter={'text': {'$exists': True}})})}
        ).end()
        list(q.all())

def test_selectinquery(n):
    """ Test with selectinquery() """
    MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = True
    run_query(n)

def test_joinedload(n):
    """ Test with joinedload() """
    MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = False
    run_query(n)

# Run
print('Running tests...')
res = benchmark_parallel_funcs(
    N_REPEATS, 10,
    test_joinedload,
    test_selectinquery
)

# Done
print(res)
