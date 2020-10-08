"""
Test: mapping DB objects directly to dicts.

I've run the following benchmark: we load 100 Users, each having 5 
articles, each having 3 comments. 
We use several different ways to do it, and compare the results.

The results are quite interesting:

test_joinedload: 15.40s (395.12%)
test_selectinload: 18.73s (480.50%)
test_core__left_join_with_python_nesting: 3.90s (100.00%)
test_core__3_queries__tuples: 4.57s (117.38%)
test_core__3_queries__json: 10.98s (281.63%)
test_subquery_jsonb_tuples: 12.35s (316.98%)
test_subquery_jsonb_objects: 20.82s (534.27%)
test_subquery_json_tuples: 6.05s (155.31%)
test_subquery_json_objects: 9.44s (242.32%)
test_single_line_agg__json: 7.31s (187.58%)
test_single_line_agg__jsonb: 14.42s (369.89%)
test_semisingle_line_agg__json: 7.34s (188.41%)
test_semisingle_line_agg__jsonb: 15.40s (395.09%)


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
from tests.models import get_big_db_for_benchmarks, User, Article

# Init DB
engine, Session = get_big_db_for_benchmarks(n_users=100,
                                            n_articles_per_user=5,
                                            n_comments_per_article=3)

# Prepare
N_REPEATS = 500
ssn = Session()


# Tests
def test_selectinload(n):
    """ Load Users+Articles+Comments, with selectinload() """
    for i in range(n):
        users = ssn.query(User).options(
            selectinload(User.articles).selectinload(Article.comments)
        ).all()


def test_joinedload(n):
    """ Load Users+Articles+Comments, with joinedload() """
    for i in range(n):
        users = ssn.query(User).options(
            joinedload(User.articles).joinedload(Article.comments)
        ).all()



def test_core__left_join_with_no_post_processing(n):
    """ Use a plain SQL query with LEFT JOIN + generate JSON objects in Python """
    for i in range(n):
        rows = ssn.execute("""
        SELECT u.*, a.*, c.* 
        FROM u
        LEFT JOIN a ON u.id = a.uid
        LEFT JOIN c ON c.aid = a.id
        """).fetchall()


def test_core__left_join_with_python_nesting(n):
    """ Use a plain SQL query with LEFT JOIN + generate JSON objects in Python """
    for i in range(n):
        rows = ssn.execute("""
        SELECT u.*, a.*, c.* 
        FROM u
        LEFT JOIN a ON u.id = a.uid
        LEFT JOIN c ON c.aid = a.id
        """).fetchall()

        # Okay, that was fast; what if we collect all the data in Python?
        # Will Python loops kill all performance?

        users = []
        user_id_to_user = {}
        article_id_to_article = {}
        comment_id_to_comment = {}

        for row in rows:
            # user dict()
            user_id = row[0]
            if user_id in user_id_to_user:
                user = user_id_to_user[user_id]
            else:
                user = {'id': row[0], 'name': row[1], 'tags': row[2], 'age': row[3], 'articles': []}
                user_id_to_user[user_id] = user
                users.append(user)

            # article dict()
            article_id = row[4]
            if article_id:
                if article_id in article_id_to_article:
                    article = article_id_to_article[article_id]
                else:
                    article = {'id': row[4], 'uid': row[5], 'title': row[6], 'theme': row[7], 'data': row[8], 'comments': []}
                    article_id_to_article[article_id] = article
                if user:
                    user['articles'].append(article)
            else:
                article = None

            # comment dict()
            comment_id = row[9]
            if comment_id:
                if comment_id in comment_id_to_comment:
                    comment = comment_id_to_comment[comment_id]
                else:
                    comment ={'id': row[9], 'aid': row[10], 'uid': row[11], 'text': row[12]}
                if article:
                    article['comments'].append(comment)
            else:
                comment = None


def test_core__3_queries__tuples(n):
    """ Make 3 queries, load tuples """
    for i in range(n):
        users = ssn.execute('SELECT u.* FROM u;').fetchall()

        user_ids = set(str(u.id) for u in users)
        articles = ssn.execute('SELECT a.* FROM a WHERE uid IN (' + (','.join(user_ids)) + ')').fetchall()

        article_ids = set(str(a.id) for a in articles)
        comments = ssn.execute('SELECT c.* FROM c WHERE aid IN (' + (','.join(article_ids)) + ')').fetchall()


def test_core__3_queries__json(n):
    """ Make 3 queries, load json rows """
    for i in range(n):
        users = ssn.execute('SELECT to_json(u) FROM u;').fetchall()

        user_ids = set(str(u[0]['id']) for u in users)
        articles = ssn.execute('SELECT to_json(a) FROM a WHERE uid IN (' + (','.join(user_ids)) + ')').fetchall()

        article_ids = set(str(a[0]['id']) for a in articles)
        comments = ssn.execute('SELECT to_json(c) FROM c WHERE aid IN (' + (','.join(article_ids)) + ')').fetchall()

# Now do the same with with nested JSON
# We query Users, their Articles, and their Comments, all as nested objects
# We use `AGG_FUNCTION`, which is either json_agg(), or jsonb_agg()
# We use different `*_SELECT` expressions, which are either a list of columns, or to_json(row), or to_jsonb(row)
# All those combinations we test to see which one is faster
NESTED_AGG_QUERY_TEMPLATE = """
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

def test_subquery_jsonb_tuples(n):
    """ Test making JSONB on the server. Return tuples. """
    query = NESTED_AGG_QUERY_TEMPLATE.format(
        # Use JSONB for nested objects
        AGG_FUNCTION='jsonb_agg',
        # Select rows as tuples
        USERS_SELECT='users.*',
        ARTICLES_SELECT='articles.*',
        COMMENTS_SELECT='comments.*',
    )
    for i in range(n):
        users = ssn.execute(query).fetchall()


def test_subquery_jsonb_objects(n):
    """ Test making JSONB on the server. Make objects with row_to_json() """
    query = NESTED_AGG_QUERY_TEMPLATE.format(
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

def test_subquery_json_tuples(n):
    """ Test making JSON on the server. Return tuples. """
    query = NESTED_AGG_QUERY_TEMPLATE.format(
        # Use JSON for nested objects
        AGG_FUNCTION='json_agg',
        # Select rows as tuples
        USERS_SELECT='users.*',
        ARTICLES_SELECT='articles.*',
        COMMENTS_SELECT='comments.*',
    )
    for i in range(n):
        users = list(ssn.execute(query))

def test_subquery_json_objects(n):
    """ Test making JSONB on the server. Make objects with row_to_json() """
    query = NESTED_AGG_QUERY_TEMPLATE.format(
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


LINE_AGG_QUERY_TEMPLATE = """
SELECT {TO_JSON}(u), {JSON_AGG}(a), {JSON_AGG}(c) 
FROM u 
    LEFT JOIN a ON(u.id=a.uid) 
    LEFT JOIN c ON (a.id=c.aid) 
GROUP BY u.id;
"""

def test_single_line_agg__json(n):
    """ Linear aggregation: no nesting; everything's aggregated into separate JSON lists """
    query = LINE_AGG_QUERY_TEMPLATE.format(
        TO_JSON='to_json',
        JSON_AGG='json_agg',
    )
    for i in range(n):
        rows = ssn.execute(query).fetchall()

def test_single_line_agg__jsonb(n):
    """ Linear aggregation: no nesting; everything's aggregated into separate JSONB lists """
    query = LINE_AGG_QUERY_TEMPLATE.format(
        TO_JSON='to_jsonb',
        JSON_AGG='jsonb_agg',
    )
    for i in range(n):
        rows = ssn.execute(query).fetchall()


SEMILINE_AGG_QUERY_TEMPLATE = """
SELECT 
    {JSON_BUILD_OBJECT}(
        'id', u.id, 'name', u.name, 'tags', u.tags, 'age', u.age, 
        'articles', {JSON_AGG}(a)), 
    {JSON_AGG}(c) 
FROM u 
    LEFT JOIN a ON(u.id=a.uid) 
    LEFT JOIN c ON (a.id=c.aid) 
GROUP BY u.id;
"""

def test_semisingle_line_agg__json(n):
    """ Aggregate only 1st level objects; things that are nested deeper are expelled to the outskirts """
    query = SEMILINE_AGG_QUERY_TEMPLATE.format(
        JSON_BUILD_OBJECT='json_build_object',
        JSON_AGG='json_agg',
    )
    for i in range(n):
        rows = ssn.execute(query).fetchall()


def test_semisingle_line_agg__jsonb(n):
    """ Aggregate only 1st level objects; things that are nested deeper are expelled to the outskirts """
    query = SEMILINE_AGG_QUERY_TEMPLATE.format(
        JSON_BUILD_OBJECT='jsonb_build_object',
        JSON_AGG='jsonb_agg',
    )
    for i in range(n):
        rows = ssn.execute(query).fetchall()



# Run
res = benchmark_parallel_funcs(
    N_REPEATS, 10,
    test_joinedload,
    test_selectinload,
    test_core__left_join_with_no_post_processing,
    test_core__left_join_with_python_nesting,
    test_core__3_queries__tuples,
    test_core__3_queries__json,
    test_subquery_jsonb_tuples,
    test_subquery_jsonb_objects,
    test_subquery_json_tuples,
    test_subquery_json_objects,
    test_single_line_agg__json,
    test_single_line_agg__jsonb,
    test_semisingle_line_agg__json,
    test_semisingle_line_agg__jsonb,
)

# Done
print(res)
