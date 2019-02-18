
class BaseMongoSqlException(AssertionError):
    pass


class InvalidQueryError(BaseMongoSqlException):
    """ Invalid input provided by the User """

    def __init__(self, err):
        super(InvalidQueryError, self).__init__('Query object error: {err}'.format(err=err))


class InvalidColumnError(BaseMongoSqlException):
    """ Query mentioned an invalid column name """

    def __init__(self, model, column_name, where):
        self.model = model
        self.column_name = column_name
        self.where = where

        super(InvalidColumnError, self).__init__(
            'Invalid column "{column_name}" for "{model}" specified in {where}'.format(
                column_name=column_name,
                model=model,
                where=where)
        )

class InvalidRelationError(InvalidColumnError):
    """ Query mentioned an invalid relationship name """
