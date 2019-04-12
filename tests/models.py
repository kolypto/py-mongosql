from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session, column_property

from sqlalchemy.sql.expression import and_
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy import Column, String, Integer, Boolean, DateTime, Table
from sqlalchemy.orm import relationship, backref, remote, foreign
from sqlalchemy.sql.schema import ForeignKey

from sqlalchemy.dialects import postgresql as pg
from sqlalchemy.ext.hybrid import hybrid_property

from mongosql import MongoSqlBase

from flask_jsontools import JsonSerializableBase
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

# TODO: test column_property() behavior. Treat it as a @property? (default exclude)

class User(Base):
    __tablename__ = 'u'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    tags = Column(pg.ARRAY(String))  # ARRAY field
    age = Column(Integer)

    @property
    def user_calculated(self):
        return self.age + 10

    def __repr__(self):
        return 'User(id={}, name={!r})'.format(self.id, self.name)

class Article(Base):
    __tablename__ = 'a'

    id = Column(Integer, primary_key=True)
    uid = Column(Integer, ForeignKey(User.id))
    title = Column(String)
    theme = Column(String)
    data = Column(pg.JSON)  # JSON field

    user = relationship(User, backref=backref('articles'))

    @property
    def calculated(self):
        return len(self.title) + self.uid

    @hybrid_property
    def hybrid(self):
        return self.id > 10 and self.user.age > 18

    @hybrid.expression
    def hybrid(cls):
        return and_(cls.id > 10, cls.user.has(User.age > 18))


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


class Edit(Base):
    __tablename__ = 'e'

    id = Column(Integer, primary_key=True)

    uid = Column(Integer, ForeignKey(User.id))
    cuid = Column(Integer, ForeignKey(User.id))
    description = Column(String)

    user = relationship(User, foreign_keys=uid)
    creator = relationship(User, foreign_keys=cuid)


class ManyFieldsModel(Base):
    """ A table with many, many columns

        Goal: convenience to test many filters in one query
    """
    # A model with many fields for testing huge filters
    __tablename__ = 'm'
    id = Column(Integer, primary_key=True)

    # Integers
    a = Column(Integer)
    b = Column(Integer)
    c = Column(Integer)
    d = Column(Integer)
    e = Column(Integer)
    f = Column(Integer)
    g = Column(Integer)
    h = Column(Integer)
    i = Column(Integer)
    j = Column(Integer)
    k = Column(Integer)

    # Arrays
    aa = Column(pg.ARRAY(String))
    bb = Column(pg.ARRAY(String))
    cc = Column(pg.ARRAY(String))
    dd = Column(pg.ARRAY(String))
    ee = Column(pg.ARRAY(String))
    ff = Column(pg.ARRAY(String))
    gg = Column(pg.ARRAY(String))
    hh = Column(pg.ARRAY(String))
    ii = Column(pg.ARRAY(String))
    jj = Column(pg.ARRAY(String))
    kk = Column(pg.ARRAY(String))

    # JSONs
    j_a = Column(pg.JSON)
    j_b = Column(pg.JSON)
    j_c = Column(pg.JSON)
    j_d = Column(pg.JSON)
    j_e = Column(pg.JSON)
    j_f = Column(pg.JSON)
    j_g = Column(pg.JSON)
    j_h = Column(pg.JSON)
    j_i = Column(pg.JSON)
    j_j = Column(pg.JSON)
    j_k = Column(pg.JSON)


class GirlWatcherFavorites(Base):
    __tablename__ = 'gwf'
    gw_id = Column(Integer, ForeignKey("gw.id"), primary_key=True)
    user_id = Column(Integer, ForeignKey("u.id"), primary_key=True)
    best = Column(Boolean)


class GirlWatcher(Base):
    """ Complex joins, custom conditions, many-to-many

        Goal: test how MongoSql handles many-to-many relationships
    """
    __tablename__ = 'gw'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    age = Column(Integer)

    favorite_id = Column(Integer, ForeignKey(User.id))

    favorite = relationship(User, foreign_keys=favorite_id)

    best = relationship(User, uselist=True, viewonly=True,
                        secondary=GirlWatcherFavorites.__table__,
                        primaryjoin= and_(id == GirlWatcherFavorites.gw_id,
                                          GirlWatcherFavorites.best == True),
                        secondaryjoin= GirlWatcherFavorites.user_id == User.id,
                        )
    good = relationship(User, uselist=True, viewonly=True,
                        secondary=GirlWatcherFavorites.__table__,
                        primaryjoin= and_(id == GirlWatcherFavorites.gw_id,
                                          GirlWatcherFavorites.best == False),
                        secondaryjoin= GirlWatcherFavorites.user_id == User.id,
                        )


class CreationTimeMixin(object):
    """ Inheritance tests: a mixin """
    ctime = Column(DateTime, doc="Creation time")

    @declared_attr
    def cuid(cls):
        return Column(Integer, ForeignKey(User.id, ondelete='SET NULL'),
                      nullable=True, doc="Created by")

    @declared_attr
    def cuser(cls):
        return relationship('User', remote_side=User.id,
                            foreign_keys='{}.cuid'.format(cls.__name__), doc="Created by")


class SpecialMixin(object):
    @property
    def get_42(self):
        return 42

    @hybrid_property
    def hyb_big_id(self):
        return self.id > 1000

    @hyb_big_id.expression
    def hyb_big_id(cls):
        return and_(cls.id > 1000)


class CarArticle(Article, CreationTimeMixin, SpecialMixin):
    """ Inheritance tests: inherit attrs """
    __tablename__ = 'ia'
    id = Column(Integer, ForeignKey(Article.id), primary_key=True)
    car = relationship('Cars', back_populates='article')


class Cars(Base):
    """ Inheritance tests: joined table inheritance + mixin """
    __tablename__ = 'ic'  # inheritance: cars

    id = Column(Integer, primary_key=True)
    type = Column(String(50))

    make = Column(String(50))
    model = Column(String(50))
    horses = Column(Integer)

    article_id = Column(ForeignKey(CarArticle.id))
    article = relationship(CarArticle, back_populates='car')

    __mapper_args__ = {
        'polymorphic_identity': 'car',
        'polymorphic_on': type
    }


class GasolineCar(Cars):
    """ Inheritance tests: joined table inheritance """
    __tablename__ = 'icg'

    id = Column(Integer, ForeignKey(Cars.id), primary_key=True)
    engine_volume = Column(Integer)

    __mapper_args__ = {
        'polymorphic_identity': 'gasoline',
    }


class ElectricCar(Cars):
    """ Inheritance tests: joined table inheritance """
    __tablename__ = 'ice'

    id = Column(Integer, ForeignKey(Cars.id), primary_key=True)
    batt_capacity = Column(Integer)

    __mapper_args__ = {
        'polymorphic_identity': 'electric',
    }


class ConfiguredLazyloadModel(Base):
    """ A model with relationhips configured to lazy=joined """
    __tablename__ = 'll'

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("u.id"))
    article_id = Column(Integer, ForeignKey("a.id"))

    # lazy
    user = relationship(User, foreign_keys=user_id, lazy='joined')
    article = relationship(Article, foreign_keys=article_id, lazy='joined')

    # not lazy
    comment_id = Column(Integer, ForeignKey("c.id"))
    comment = relationship(Comment, foreign_keys=comment_id)







def init_database(autoflush=True):
    """ Init DB
    :rtype: (sqlalchemy.engine.Engine, sqlalchemy.orm.Session)
    """
    engine = create_engine('postgresql://postgres:postgres@localhost/test_mongosql', convert_unicode=True, echo=False)
    Session = sessionmaker(autocommit=autoflush, autoflush=autoflush, bind=engine)
    return engine, Session


def create_all(engine):
    """ Create all tables """
    Base.metadata.create_all(bind=engine)


def drop_all(engine):
    """ Drop all tables """
    Base.metadata.drop_all(bind=engine)


def content_samples():
    """ Generate content samples """
    return [[
        User(id=1, name='a', age=18, tags=['1', 'a']),
        User(id=2, name='b', age=18, tags=['2', 'a', 'b']),
        User(id=3, name='c', age=16, tags=['3', 'a', 'b', 'c']),

        Article(id=10, uid=1, title='10', data={'rating': 5  , 'o': {'a': True}}),
        Article(id=11, uid=1, title='11', data={'rating': 5.5, 'o': {'a': True}}),
        Article(id=12, uid=1, title='12', data={'rating': 6  , 'o': {'a': False}}),
        Article(id=20, uid=2, title='20', data={'rating': 4.5, 'o': {'a': False}}),
        Article(id=21, uid=2, title='21', data={'rating': 4  , 'o': {'z': True}}),
        Article(id=30, uid=3, title='30', data={               'o': {'z': False}}),

        Comment(id=100, aid=10, uid=1, text='10-a'),
        Comment(id=101, aid=10, uid=2, text='10-b'),
        Comment(id=102, aid=10, uid=3, text='10-c'),
        Comment(id=103, aid=11, uid=1, text='11-a'),
        Comment(id=104, aid=11, uid=2, text='11-b'),
        Comment(id=105, aid=12, uid=1, text='12-a'),
        Comment(id=106, aid=20, uid=1, text='20-a-ONE'),
        Comment(id=107, aid=20, uid=1, text='20-a-TWO'),
        Comment(id=108, aid=21, uid=1, text='21-a'),

        GirlWatcher(id=1, name='Fred', age=65, favorite_id=3),
        GirlWatcher(id=2, name='Ban', age=55, favorite_id=2),
    ], [
        GirlWatcherFavorites(gw_id=1, user_id=2, best=False),
        GirlWatcherFavorites(gw_id=1, user_id=3, best=True),
        GirlWatcherFavorites(gw_id=2, user_id=1, best=False),
        GirlWatcherFavorites(gw_id=2, user_id=2, best=True),
        GirlWatcherFavorites(gw_id=2, user_id=3, best=False),
    ]]


def content_samples_random(n_users, n_articles_per_user, n_comments_per_article):
    """ Generate lots of users with lots of articles with lots of comments """
    ret = []
    for i in range(n_users):
        ret.append(User(name='X', age=50,
                        articles=[
                            Article(title='X'*20,
                                    comments=[
                                        Comment(text='X'*100)
                                        for ic in range(n_comments_per_article)
                                    ])
                            for ia in range(n_articles_per_user)
                        ]))
    return ret


def get_big_db_for_benchmarks(n_users, n_articles_per_user, n_comments_per_article):
    # Connect, create tables
    engine, Session = init_database(autoflush=True)
    drop_all(engine)
    create_all(engine)

    # Fill DB
    ssn = Session()
    ssn.begin()
    ssn.add_all(content_samples_random(n_users, n_articles_per_user, n_comments_per_article))
    ssn.commit()

    # Done
    return engine, Session



def get_empty_db(autoflush=True):
    # Connect, create tables
    engine, Session = init_database(autoflush=autoflush)
    drop_all(engine)
    create_all(engine)
    return engine, Session


def get_working_db_for_tests(autoflush=True):
    # Connect, create tables
    engine, Session = get_empty_db(autoflush=autoflush)

    # Fill DB
    ssn = Session()
    for entities_list in content_samples():
        if autoflush:
            ssn.begin()
        ssn.add_all(entities_list)
        ssn.commit()

    # Done
    return engine, Session


if __name__ == '__main__':
    # Developer's playground!
    import logging
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

    engine, Session = get_working_db_for_tests()

    # Useful imports and variables
    ssn = Session()

    from util import q2sql
    from mongosql import MongoQuery
    from sqlalchemy import inspect, func
    from sqlalchemy.orm import Query
    from sqlalchemy.orm.base import instance_state
    from sqlalchemy.orm import Load, defaultload, lazyload, immediateload, selectinload
    from sqlalchemy.orm import raiseload, noload, load_only, defer, undefer
    from sqlalchemy.orm import aliased, contains_eager, contains_alias

    print('\n'*10)

    from IPython import embed ; embed()
