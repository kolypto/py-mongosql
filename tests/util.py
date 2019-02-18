import sys
from sqlalchemy.dialects import postgresql as pg

PY2 = sys.version_info[0] == 2

def stmt2sql(stmt):
    """ Convert an SqlAlchemy statement into a string """
    # See: http://stackoverflow.com/a/4617623/134904
    # This intentionally does not escape values!
    dialect = pg.dialect()
    query = stmt.compile(dialect=dialect)
    if PY2:
        return (query.string.encode(dialect.encoding) % query.params).decode(dialect.encoding)
    else:
        return query.string % query.params


def q2sql(q):
    """ Convert an SqlAlchemy query to string """
    return stmt2sql(q.statement)
