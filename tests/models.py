from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, String, Integer
from sqlalchemy.orm import relationship, backref
from sqlalchemy.sql.schema import ForeignKey

from sqlalchemy.dialects import postgresql as pg

from mongosql import MongoSqlBase

from flask.ext.jsontools import JsonSerializableBase
from flask import json


class MongoJsonSerializableBase(JsonSerializableBase):
    """ Declarative Base mixin to allow objects serialization

        Defines interfaces utilized by :cls:ApiJSONEncoder
    """
    mongo_project_properties = None
    join_project_properties = None

    def _project_join(self, obj, project):
        if getattr(obj, '__json__', None):
            data = obj.__json__()
        else:
            data = json.loads(json.dumps(obj))
        for name, include in project.items():
            if include:
                data[name] = getattr(obj, name)
        return data

    def __json__(self, exluded_keys=set()):
        data = super(MongoJsonSerializableBase, self).__json__(exluded_keys)
        if self.mongo_project_properties:
            for name, include in self.mongo_project_properties.items():
                if isinstance(include, dict):
                    if name in data:
                        obj = data[name]
                        if isinstance(obj, list):
                            data[name] = [self._project_join(i, include) for i in obj]
                        else:
                            data[name] = self._project_join(obj, include)
                else:
                    if include:
                        data[name] = getattr(self, name)
        return data


Base = declarative_base(cls=(MongoSqlBase, MongoJsonSerializableBase))


class User(Base):
    __tablename__ = 'u'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    tags = Column(pg.ARRAY(String))  # ARRAY field
    age = Column(Integer)

    @property
    def user_calculated(self):
        return self.age + 10


class Article(Base):
    __tablename__ = 'a'

    id = Column(Integer, primary_key=True)
    uid = Column(Integer, ForeignKey(User.id))
    title = Column(String)
    data = Column(pg.JSON)  # JSON field

    user = relationship(User, backref=backref('articles'))

    @property
    def calculated(self):
        return len(self.title) + self.uid


class Comment(Base):
    __tablename__ = 'c'

    id = Column(Integer, primary_key=True)

    aid = Column(Integer, ForeignKey(Article.id))
    uid = Column(Integer, ForeignKey(User.id))

    text = Column(String)

    article = relationship(Article, backref=backref("comments"))
    user = relationship(User, backref=backref("comments"))

    @property
    def comment_calc(self):
        return self.text[-3:]


class Role(Base):
    __tablename__ = 'r'

    id = Column(Integer, primary_key=True)

    uid = Column(Integer, ForeignKey(User.id))
    title = Column(String)
    description = Column(String)

    user = relationship(User, backref=backref("roles"))


def init_database():
    """ Init DB
    :rtype: (sqlalchemy.engine.Engine, sqlalchemy.orm.Session)
    """
    engine = create_engine('postgresql://postgres:postgres@localhost/test_mongosql', convert_unicode=True, echo=False)
    Session = sessionmaker(autocommit=True, autoflush=True, bind=engine)
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

        Article(id=10, uid=1, title='10', data={'rating': 5  , 'o': {'a': True}}),
        Article(id=11, uid=1, title='11', data={'rating': 5.5, 'o': {'a': True}}),
        Article(id=12, uid=1, title='12', data={'rating': 6  , 'o': {'a': False}}),
        Article(id=20, uid=2, title='20', data={'rating': 4.5, 'o': {'a': False}}),
        Article(id=21, uid=2, title='21', data={'rating': 4  , 'o': {'z': True}}),
        Article(id=30, uid=3, title='30', data={               'o': {'z': False}}),

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

    from sqlalchemy import inspect, func
    from sqlalchemy.orm import noload, load_only, defaultload, lazyload, immediateload, aliased, contains_eager, contains_alias

    ssn.query(User).filter_by(id=999).delete()

    u1 = User(id=999, name=999)
    u2 = User(id=999, name=999) ; ssn.add(u2)
    u3 = ssn.query(User).filter_by(id=1).one()
    u4 = ssn.query(User).options(load_only(User.name)).filter_by(id=2).one() ; u4.tags = [1,2,3]

    #ssn.begin()
    #ssn.commit()


    s1, s2, s3, s4 = map(inspect, (u1, u2, u3, u4))

    from IPython import embed ; embed()
