from time import time
from sqlalchemy.orm import joinedload, selectinload

# Run me:
# $ python -m cProfile -o profile.out tests/benchmark.py && gprof2dot -f pstats profile.out | dot -Tpng -o profile.png && gwenview profile.png

from tests.models import *
try:
    from mongosql.handlers import MongoJoin
    from mongosql import selectinquery
    SELECTINQUERY_SUPPORTED = True
except ImportError:
    # This is to reuse this benchmark with the old MongoSql 1.x
    SELECTINQUERY_SUPPORTED = False


# Timer class
class timer(object):
    def __init__(self):
        self.total = 0.0
        self._current = time()

    def stop(self, ):
        self.total += time() - self._current
        return self

# Take the scariest query with joins, and execute it many times over
N_QUERIES = 1000
FOCUS_ON_MONGOSQL = False  # skip all row processing ; just build the query


for big_db in (False, True) if not FOCUS_ON_MONGOSQL else (None,):
    # Init the DB
    print()
    if big_db is None:
        print('Test: focus on MongoSQL, no data')
        engine, Session = get_empty_db()
    elif big_db is True:
        print('Test: with a large result set')
        engine, Session = get_big_db_for_benchmarks(n_users=100, n_articles_per_user=5, n_comments_per_article=3)
    else:
        print('Test: with a small result set')
        engine, Session = get_working_db_for_tests()

    # Session
    ssn = Session()

    # Test sqlalchemy: joinedload, selectinload, selectinquery
    if not FOCUS_ON_MONGOSQL and 'test-sqlalchemy':
        joinedload_timer = timer()
        for i in range(N_QUERIES):
            q = ssn.query(User).options(joinedload(User.articles).joinedload(Article.comments))
            list(q.all())
        joinedload_timer.stop()
        print(f'SqlAlchemy, joinedload: {joinedload_timer.total:.02f}s')

        selectinload_timer = timer()
        for i in range(N_QUERIES):
            q = ssn.query(User).options(selectinload(User.articles).selectinload(Article.comments))
            list(q.all())
        selectinload_timer.stop()
        print(f'SqlAlchemy, selectinload: {selectinload_timer.total:.02f}s')

        if SELECTINQUERY_SUPPORTED:
            selectinquery_timer = timer()
            for i in range(N_QUERIES):
                q = ssn.query(User).options(selectinquery(User.articles, lambda q, **kw: q).selectinquery(Article.comments, lambda q, **kw: q))
                list(q.all())
            selectinquery_timer.stop()
            print(f'SqlAlchemy, selectinquery: {selectinquery_timer.total:.02f}s')


    # The benchmark itself
    selectinquery_states = (False, True) if SELECTINQUERY_SUPPORTED else (False,)
    for selectinquery_enabled in selectinquery_states:
        # Enable/Disable selectinquery()
        if SELECTINQUERY_SUPPORTED:
            MongoJoin.ENABLED_EXPERIMENTAL_SELECTINQUERY = selectinquery_enabled

        # Prepare the list of results
        qs = [None for i in range(N_QUERIES)]

        # Benchmark
        total_timer = timer()
        mongosql_timer = timer()
        for i in range(len(qs)):
            # using query from test_join__one_to_many()
            mq = User.mongoquery(ssn).query(
                project=['name'],
                filter={'age': {'$ne': 100}},
                join={'articles': dict(project=['title'],
                                        filter={'theme': {'$ne': 'sci-fi'}},
                                        join={'comments': dict(project=['aid'],
                                                                filter={'text': {'$exists': True}})})}
            )
            qs[i] = mq.end()
        mongosql_timer.stop()

        sqlalchemy_timer = timer()
        if not FOCUS_ON_MONGOSQL and 'sqlalchemy-load':
            for q in qs:
                list(q.all())  # load all, force sqlalchemy to process every row
        sqlalchemy_timer.stop()
        total_timer.stop()

        ms_per_query = total_timer.total / N_QUERIES * 1000

        print(f'MongoSql, {"selectinquery" if selectinquery_enabled else "left join"}: {total_timer.total:0.2f}s '
              f'(mongosql: {mongosql_timer.total:0.2f}s, sqlalchemy: {sqlalchemy_timer.total:0.2f}s), {ms_per_query:.02f}ms/query')

# Current run time with 3000 queries, Python 3.7

# Test: with a small result set (~10 rows * few related)
# with selectinquery: 4.64s (mongosql: 0.67s, sqlalchemy: 3.98s), 4.64ms/query
# without selectinquery: 7.74s (mongosql: 4.78s, sqlalchemy: 2.95s), 7.74ms/query
#
# Test: with a large result set (100 rows * 5 related * 3 related)
# with selectinquery: 108.24s (mongosql: 1.45s, sqlalchemy: 106.79s), 108.24ms/query
# without selectinquery: 102.00s (mongosql: 5.10s, sqlalchemy: 96.90s), 102.00ms/query
