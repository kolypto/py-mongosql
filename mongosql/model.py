from sqlalchemy import inspect

from .statements import MongoProjection, MongoSort, MongoGroup, MongoCriteria, MongoJoin, MongoAggregate


class MongoModel(object):
    """ Sqlalchemy Model wrapper that generates query chunks """

    def __init__(self, model):
        """ Create MongoSql from a model

            :type model: sqlalchemy.ext.declarative.declarative_base
            :param model: The model to build queries for
        """
        ins = inspect(model)

        # : The model we're working with
        self.__model = model

        #: Model columns
        self.__model_columns = {name: getattr(model, name) for name, c in ins.column_attrs.items()}

        #: Model relations
        self.__model_relations = {name: getattr(model, name) for name, c in ins.relationships.items()}

    @property
    def model(self):
        """ Get model
        :rtype: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        return self.__model

    @property
    def model_columns(self):
        """ Get model columns
        :return: {name: Column}
        :rtype: dict[sqlalchemy.orm.properties.ColumnProperty]
        """
        return self.__model_columns

    @property
    def model_relations(self):
        """ Get model relations
        :return: {name: Relation}
        :rtype: dict[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        return self.__model_relations

    #region Wrappers

    def project(self, projection):
        """ Build projection for a Query

        :type projection: None | str | dict | list | tuple
        :param projection: Projection spec
        :returns: Query options to load specific columns
            Usage:
                p = MongoModel(User).project(['login', 'email'])
                query.options(p)
        :rtype: sqlalchemy.orm.Load
        :raises AssertionError: invalid input
        :raises AssertionError: unknown column name
        """
        return MongoProjection(projection)(self)

    def sort(self, sort_spec):
        """ Build sorting for a Query

            :type sort_spec: None | str | list | tuple | OrderedDict
            :param sort_spec: Sort spec
            :returns: List of columns, with ordering modifiers applied.
                Usage:
                    s = MongoModel(User).sort(['state+'])
                    query.order_by(*s)
            :rtype: sqlalchemy.orm.query.Query
            :raises AssertionError: invalid input
            :raises AssertionError: unknown column name
        """
        return MongoSort(sort_spec)(self)

    def group(self, group_spec):
        """ Build grouping for a Query

            See :meth:sort()

            :returns: List of columns, with grouping modifiers applied.
                Usage:
                    s = MongoModel(User).group(['state'])
                    query.group_by(*s)
            :rtype: sqlalchemy.orm.query.Query
            :raises AssertionError: invalid input
            :raises AssertionError: unknown column name
        """
        return MongoGroup(group_spec)(self)

    def filter(self, criteria):
        """ Build filtering condition for a Query

            :type criteria: None, dict
            :param criteria: The criteria to filter with
            :rtype: sqlalchemy.sql.elements.BooleanClauseList
            :returns: Filtering conditions.
                Usage:
                    c = MongoModel(User).filter({ 'id': 1 })
                    query.filter(c)
            :raises AssertionError: invalid input
            :raises AssertionError: unknown column name
        """
        return MongoCriteria(criteria)(self)

    def skip(self, skip=None):
        """ Build the rows offset value

        :type skip: int | None
        :param skip: The number of rows to skip
        :returns: Offset value.
            Usage:
                skip = MongoModel(User).skip(10)
                query.offset(skip)
        :rtype: int | None
        :raises AssertionError: invalid input
        """
        assert skip is None or isinstance(skip, int), 'Skip must be one of: None, int'
        return None if skip is None or skip <= 0 else skip

    def limit(self, limit=None, skip=None):
        """ Build the rows limit & offset

            :type limit: int | None
            :param limit: Row count limitation
            :type skip: int | None
            :param skip: The number of rows to skip
            :returns: (limit, offset)
                Usage:
                    skip, limit = MongoModel(User).limit(20, 10)
                    query.limit(limit).offset(skip)
            :rtype: (int|None, int|None)
            :raises AssertionError: invalid input
        """
        assert limit is None or isinstance(limit, int), 'Limit must be one of: None, int'
        return (
            None if limit is None or limit <= 0 else limit,
            self.skip(skip)
        )

    def join(self, relnames):
        """ Build eager loader for the relations

            :type relnames: list[str]
            :param relnames: List of relations to load eagerly
            :returns: Query options to load specific columns
                Usage:
                    j = MongoModel(User).join('posts')
                    query.options(*j).with_labels()
            :rtype: list[sqlalchemy.orm.Load]
            :raises AssertionError: invalid input
        """
        return MongoJoin(relnames)(self)

    def aggregate(self, agg_spec):
        """ Select aggregated results

        :param agg_spec: Aggregation spec
        :type agg_spec: dict
        :return: List of selectables.
                Usage:
                    a = MongoModel(User).aggregate({ oldest: { $max: 'age' } })
                    query.add_columns(*a)
        :rtype: list[sqlalchemy.sql.elements.ColumnElement]
        :raises AssertionError: invalid input
        """
        return MongoAggregate(agg_spec)(self)

    #endregion
