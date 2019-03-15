from time import time
from tests.models import *

# Run me: 
# git checkout full-refactoring && python tests/benchmark.py && git checkout develop && python tests/benchmark.py && git checkout full-refactoring

# Version compatibility
try:
    init_db = init_database
except:
    init_db = get_working_db_for_tests

# Init the DB
engine, Session = init_db()
ssn = Session()

# Take the scariest query with joins, and execute it many times over
t_start = time()
for i in range(3000):
    # using query from test_join__one_to_many()
    mq = User.mongoquery(ssn).query(
        project=['name'],
        filter={'age': 18},
        join={'articles': dict(project=['title'],
                                filter={'theme': 'sci-fi'},
                                join={'comments': dict(project=['aid'],
                                                        filter={'text': {'$exists': True}})})}
    )
    q = mq.end()
    q.all()  # load all
t_total = time() - t_start
print(f'{t_total:0.2f}s')

# Current run time with 3000 queries, Python 3.7
# old MongoSQL 1.x: 49.98s, 16ms/query
# new MongoSQL 2.x: 27.88s, 9ms/query
# Performance improvement: 50%, win: 7ms/query
