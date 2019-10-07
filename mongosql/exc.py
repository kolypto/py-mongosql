
class BaseMongoSqlException(AssertionError):  # `AssertionError` for backwards-compatibility
    pass


class InvalidQueryError(BaseMongoSqlException):
    """ Invalid input provided by the User """

    def __init__(self, err: str):
        super(InvalidQueryError, self).__init__('Query object error: {err}'.format(err=err))


class DisabledError(InvalidQueryError):
    """ The feature is disabled """


class InvalidColumnError(BaseMongoSqlException):
    """ Query mentioned an invalid column name """

    def __init__(self, model: str, column_name: str, where: str):
        self.model = model
        self.column_name = column_name
        self.where = where

        super(InvalidColumnError, self).__init__(
            'Invalid column "{column_name}" for "{model}" specified in {where}'.format(
                column_name=column_name,
                model=model,
                where=where)
        )


class InvalidRelationError(InvalidColumnError, BaseMongoSqlException):
    """ Query mentioned an invalid relationship name """
    def __init__(self, model: str, column_name: str, where: str):
        self.model = model
        self.column_name = column_name
        self.where = where

        super(InvalidColumnError, self).__init__(
            'Invalid relation "{column_name}" for "{model}" specified in {where}'.format(
                column_name=column_name,
                model=model,
                where=where)
        )


class RuntimeQueryError(BaseMongoSqlException):
    """ Uncaught error while processing a MongoQuery

    This class is used to augment other errors
    """
