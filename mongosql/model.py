from sqlalchemy import inspect

from .statements import MongoProjection, MongoSort, MongoGroup, MongoCriteria, MongoJoin


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
        self.__model_columns = {c.name: getattr(model, c.name) for c in ins.mapper.column_attrs}

    @property
    def model_columns(self):
        """ Get model columns
        :return: {name: Column}
        :rtype: dict(str, sqlalchemy.sql.schema.Column)
        """
        return self.__model_columns

    #region Wrappers

    def project(self, projection):
        """ Build projection for a Query

        :type projection: None | str | dict | list | tuple
        :param projection: Projection spec
        :returns: List of columns to include in the query.
            Usage:
                p = MongoModel(User).project(['login', 'email'])
                query.options(*p)
        :rtype: list(sqlalchemy.sql.schema.Column)
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
            :rtype: BooleanClauseList
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

            :type relnames: list(str)
            :param relnames: List of relations to load eagerly
            :returns: List of query options.
                Usage:
                    j = MongoModel(User).join('posts')
                    query.options(*j).with_labels()
            :rtype: list(loader_option)
        """
        # TODO: User filter/sort/limit/.. on list relations, as currently, it selects the list of ALL related objects!
        # TODO: Support loading sub-relations through 'user.profiles'
        return MongoJoin(relnames)(self)

    #endregion
