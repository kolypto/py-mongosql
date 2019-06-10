"""
This benchmark compares the performance of:
* selectinload()
* selectinquery() with query caching
* selectinquery() with no query caching
"""
from tests.benchmarks.benchmark_utils import benchmark_parallel_funcs
from tests.models import get_big_db_for_benchmarks, User

from mongosql import CountingQuery

# Init DB
engine, Session = get_big_db_for_benchmarks(50, 0, 0)

# Prepare
N_REPEATS = 1000
ssn = Session()


# Tests
def test_two_queries(n):
    """ Test making an additional query to get the count """
    for i in range(n):
        users = list(ssn.query(User))
        count = ssn.query(User).count()

def test_counting_query(n):
    """ Test CountingQuery """
    for i in range(n):
        qc = CountingQuery(ssn.query(User))
        users = list(qc)
        count = qc.count


# Run
res = benchmark_parallel_funcs(
    N_REPEATS, 10,
    test_two_queries,
    test_counting_query,
)

# Done
print(res)
