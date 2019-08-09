"""
Test: mapping DB objects directly to dicts.

I've run the following benchmark: we load 100 Users, each having 5 
articles, each having 3 comments. 
We use several different ways to do it, and compare the results.

The results are quite interesting:

test_load_related__joinedload: 32.22s (447.81%)  -- use SqlAlchemy + joinedload()
test_load_related__selectinload: 38.98s (541.70%) -- use SqlAlchemy + selectinload()
test_load_related__no_sqlalchemy__left_join: 7.20s (100.00%) -- use plain SQL, no ORM
test_load_related__JSONB_tuples: 25.41s (353.18%) -- form JSONB in postgres
test_load_related__JSONB_objects: 39.91s (554.60%) -- form JSONB in postgres, also use to_jsonb() on rows to get the keys
test_load_related__JSON_tuples: 11.72s (162.93%) -- form JSON in postgres
test_load_related__JSON_objects: 19.18s (266.60%) -- form JSON in postgres, also use to_json() on rows to get the keys

What that means is:

Postgres was able to load all data in 7.20s and send it to us.
With SqlAlchemy ORM, the whole process has taken 32.22 seconds: the ORM has spent an additional 25s making its Python objects!!
That's 500% overhead!

That's alright if you load just one object. But when all we need is load a bunch of objects and immediately convert
them to JSON, that's a huge, huge overhead for no added benefit.  We don't need no ORM features for this task.

Sidenote: joinedload() is somehow 20% faster than selectinload(). Surprise!
But that is probably because we didn't have many fields.

Now, @vdmit11 has suggested a crazy idea: what if we make JSON object in
Postgres? `jsonb_agg()` is what he has suggested. I've tested different ways to do it, and discovered that it really
is faster.

Using `json_agg()` is somehow 2x faster than `jsonb_agg()`, both in Postgres 9.6 and 11.5.
We can also win an additional 2x by not using `to_json()` on rows, but return tuples.
Both this techniques let us fetch the results 3.5x faster than `selectinload()`, 2.7x faster than `joinedload()`.

But this will give us tuples.
If we try to use `to_json()` and fetch keyed objects, it's more convenient, but reduces the performance improvement
to just 1.5x, which brings it close to what SqlAlchemy does.

Conclusion: forming JSON directly in Postgres can potentially speed up some queries 3x. But this is only applicable
to those queries that feed the data to JSON immediately. It's worth doing, but is rather complicated.

The problems with this approach: we'll have to make sure that `@property` fields are included into the results if
they were specified in a projection.
"""
from sqlalchemy.orm import selectinload, joinedload

from tests.benchmarks.benchmark_utils import benchmark_parallel_funcs
from tests.models import get_big_db_for_benchmarks, User, Article, Comment

# Init DB
engine, Session = get_big_db_for_benchmarks(n_users=100,
                                            n_articles_per_user=5,
                                            n_comments_per_article=3)

# Prepare
N_REPEATS = 1000
ssn = Session()


# Tests
def test_load_related__selectinload(n):
    """ Load Users+Articles+Comments, with selectinload() """
    for i in range(n):
        users = list(ssn.query(User).options(
            selectinload(User.articles).selectinload(Article.comments)
        ))

def test_load_related__no_sqlalchemy__left_join(n):
    """ Load Users+Articles+Comments: a plain SQL query to estimate SqlAlchemy's overhead. LEFT JOIN. """
    for i in range(n):
        list(ssn.execute("""
        SELECT u.*, a.*, c.* 
        FROM u
        LEFT JOIN a ON u.id = a.uid
        LEFT JOIN c ON c.aid = a.id
        """))

def test_load_related__joinedload(n):
    """ Load Users+Articles+Comments, with joinedload() """
    for i in range(n):
        users = list(ssn.query(User).options(
            joinedload(User.articles).joinedload(Article.comments)
        ))

# Now do the same with with nested JSON
# We query Users, their Articles, and their Comments, all as nested objects
# We use `AGG_FUNCTION`, which is either json_agg(), or jsonb_agg()
# We use different `*_SELECT` expressions, which are either a list of columns, or to_json(row), or to_jsonb(row)
# All those combinations we test to see which one is faster
QUERY_TEMPLATE = """
SELECT
    {USERS_SELECT},
    {AGG_FUNCTION}(articles_q) AS articles
FROM u AS users
LEFT JOIN (
    SELECT
        {ARTICLES_SELECT},
        {AGG_FUNCTION}(comments_q) AS comments
    FROM a AS articles
    LEFT JOIN (
        SELECT 
            {COMMENTS_SELECT}
        FROM c AS comments
    ) AS comments_q ON articles.id = comments_q.aid
    GROUP BY articles.id
) AS articles_q ON users.id = articles_q.uid
GROUP BY users.id;
"""

def test_load_related__JSONB_tuples(n):
    """ Test making JSONB on the server. Return tuples. """
    query = QUERY_TEMPLATE.format(
        # Use JSONB for nested objects
        AGG_FUNCTION='jsonb_agg',
        # Select rows as tuples
        USERS_SELECT='users.*',
        ARTICLES_SELECT='articles.*',
        COMMENTS_SELECT='comments.*',
    )

    for i in range(n):
        users = list(ssn.execute(query))


def test_load_related__JSONB_objects(n):
    """ Test making JSONB on the server. Make objects with row_to_json() """
    query = QUERY_TEMPLATE.format(
        # Use JSONB for nested objects
        AGG_FUNCTION='jsonb_agg',
        # Select rows as JSONB objects
        # Select ids needed for joining as well
        USERS_SELECT='users.id, to_jsonb(users) AS user',
        ARTICLES_SELECT='articles.id, articles.uid, to_jsonb(articles) AS article',
        COMMENTS_SELECT='comments.id, comments.aid, to_jsonb(comments) AS comment',
    )

    for i in range(n):
        users = list(ssn.execute(query))

def test_load_related__JSON_tuples(n):
    """ Test making JSON on the server. Return tuples. """
    query = QUERY_TEMPLATE.format(
        # Use JSON for nested objects
        AGG_FUNCTION='json_agg',
        # Select rows as tuples
        USERS_SELECT='users.*',
        ARTICLES_SELECT='articles.*',
        COMMENTS_SELECT='comments.*',
    )

    for i in range(n):
        users = list(ssn.execute(query))

def test_load_related__JSON_objects(n):
    """ Test making JSONB on the server. Make objects with row_to_json() """
    query = QUERY_TEMPLATE.format(
        # Use JSON for nested objects
        AGG_FUNCTION='json_agg',
        USERS_SELECT='users.id, to_json(users) AS user',
        # Select rows as JSON objects
        # Select ids needed for joining as well
        ARTICLES_SELECT='articles.id, articles.uid, to_json(articles) AS article',
        COMMENTS_SELECT='comments.id, comments.aid, to_json(comments) AS comment',
    )

    for i in range(n):
        users = list(ssn.execute(query))


# Run
res = benchmark_parallel_funcs(
    N_REPEATS, 10,
    test_load_related__joinedload,
    test_load_related__selectinload,
    test_load_related__no_sqlalchemy__left_join,
    test_load_related__JSONB_tuples,
    test_load_related__JSONB_objects,
    test_load_related__JSON_tuples,
    test_load_related__JSON_objects,
)

# Done
print(res)
