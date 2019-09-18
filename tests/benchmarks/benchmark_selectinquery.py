"""
This benchmark compares the performance of:
* selectinload()
* selectinquery() with query caching
* selectinquery() with no query caching
"""


from tests.benchmarks.benchmark_utils import benchmark_parallel_funcs

from sqlalchemy.orm import selectinload, joinedload

from mongosql import selectinquery
from tests.models import get_working_db_for_tests, User, Article

# Run me: PyCharm Profiler
# Run me: python -m cProfile -o profile.out tests/benchmark_selectinquery.py

# Init DB
engine, Session = get_working_db_for_tests()

# Prepare
N_REPEATS = 1000
ssn = Session()

# Tests
def test_selectinload(n):
    """ Test SqlAlchemy's selectinload(): using it as a baseline """
    for i in range(n):
        q = ssn.query(User).options(
            selectinload(User.articles).selectinload(Article.comments)
        )
        list(q.all())

def test_selectinquery__cache(n):
    """ Test our custom selectinquery(), with query caching """
    for i in range(n):
        q = ssn.query(User).options(
            selectinquery(User.articles, lambda q: q, 'a').selectinquery(Article.comments, lambda q: q, 'b')
        )
        list(q.all())

def test_selectinquery__no_cache(n):
    """ Test our custom selectinquery(), without query caching """
    for i in range(n):
        q = ssn.query(User).options(
            selectinquery(User.articles, lambda q: q).selectinquery(Article.comments, lambda q: q)
        )
        list(q.all())


# Run
res = benchmark_parallel_funcs(
    N_REPEATS, 10,
    test_selectinload,
    test_selectinquery__cache,
    test_selectinquery__no_cache,
)

# Done
print(res)
