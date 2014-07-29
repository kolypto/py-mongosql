from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.schema import ForeignKey

from sqlalchemy.dialects import postgresql as pg

from mongosql import MongoSqlBase

Base = declarative_base(cls=(MongoSqlBase,))


class User(Base):
    __tablename__ = 'u'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    tags = Column(pg.ARRAY(String))
    age = Column(Integer)


class Article(Base):
    __tablename__ = 'a'

    id = Column(Integer, primary_key=True)
    uid = Column(Integer, ForeignKey(User.id))
    title = Column(String)

    user = relationship(User, lazy='joined', backref=backref('articles', lazy='joined',))


class Comment(Base):
    __tablename__ = 'c'

    id = Column(Integer, primary_key=True)

    aid = Column(Integer, ForeignKey(Article.id))
    uid = Column(Integer, ForeignKey(User.id))

    text = Column(String)

    article = relationship(Article, lazy='joined', backref=backref("comments", lazy='joined',))
    user = relationship(User, lazy='joined', backref=backref("comments", lazy='joined',))




def init_database():
    """ Init DB
    :rtype: (sqlalchemy.engine.Engine, sqlalchemy.orm.Session)
    """
    engine = create_engine('postgresql://postgres:postgres@localhost/test_mongosql', convert_unicode=True)
    Session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    return engine, Session

def create_all(engine):
    """ Create all tables """
    Base.metadata.create_all(bind=engine)

def drop_all(engine):
    """ Drop all tables """
    Base.metadata.drop_all(bind=engine)

def content_samples():
    """ Generate content samples """
    return [
        User(id=1, name='a', age=18, tags=['1', 'a']),
        User(id=2, name='b', age=18, tags=['2', 'a', 'b']),
        User(id=3, name='c', age=16, tags=['3', 'a', 'b', 'c']),

        Article(id=10, uid=1, title='10'),
        Article(id=11, uid=1, title='11'),
        Article(id=12, uid=1, title='12'),
        Article(id=20, uid=2, title='20'),
        Article(id=21, uid=2, title='21'),
        Article(id=30, uid=3, title='30'),

        Comment(aid=10, uid=1, text='10-a'),
        Comment(aid=10, uid=2, text='10-b'),
        Comment(aid=10, uid=3, text='10-c'),
        Comment(aid=11, uid=1, text='11-a'),
        Comment(aid=11, uid=2, text='11-b'),
        Comment(aid=12, uid=1, text='12-a'),
        Comment(aid=20, uid=1, text='20-a-ONE'),
        Comment(aid=20, uid=1, text='20-a-TWO'),
        Comment(aid=21, uid=1, text='21-a'),
    ]

if __name__ == '__main__':
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    engine, Session = init_database()
    ssn = Session()

    from sqlalchemy import inspect
    from sqlalchemy.orm import noload, load_only, lazyload

    u = ssn.query(User).options(load_only('id'), lazyload('articles'), lazyload('comments')).first()
    ins = inspect(u)

    from IPython import embed ; embed()
