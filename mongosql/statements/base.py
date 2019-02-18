from __future__ import absolute_import
from ..bag import ModelPropertyBags
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class _MongoQueryStatementBase(object):
    """ An implementation of a statement from MongoQuery

        Every subclass will handle a single field from the Query object
    """

    #: Name of the QueryObject section that this object is capable of handling
    query_object_section_name = None

    def __init__(self, model):
        """ Initialize the Query Object section handler with a model.

        This method does *not* receive any input data just yet, with the purpose of having an
        object that can be extended with some interesting defaults right at init time.

        :param model: The sqlalchemy model it's being applied to
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        #: The model to build the statement for
        self.model = model
        #: Model property bags: because we need access to the lists of its properties
        self.bags = ModelPropertyBags.for_model_or_alias(model)
        #: A CombinedBag() that allows to handle properties of multiple types (e.g. columns + hybrid properties)
        self.supported_bags = self._get_supported_bags()

    def __copy__(self):
        """ Some objects may be reused: i.e. their state before input() is called.

        Reusable statements are implemented using the Reusable() wrapper which performs the
        automatic copying on input() call
        """
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def _get_supported_bags(self):
        """ Get the _PropertiesBag interface supported by this statement

        :rtype: mongosql.bag.PropertiesBagBase
        """
        raise NotImplementedError()

    def validate_properties(self, prop_names, bag=None, where=None):
        """ Validate the given list of property names against `self.supported_bags`

        :param prop_names: List of property names
        :param bag: A specific bag to use
        :raises InvalidColumnError
        """
        # Bag to check against
        if bag is None:
            bag = self.supported_bags

        # Validate
        invalid = bag.get_invalid_names(prop_names)
        if invalid:
            raise InvalidColumnError(self.bags.model_name,
                                     invalid.pop(),
                                     where or self.query_object_section_name)

    def input(self, qo_value):
        """ Get a section of the Query object.

        The purpose of this method is to receive the input, validate it, and store as a public
        property so that external tools may export its value.
        Note that validation does not *have* to happen here: it may in fact be implemented in one
        of the compile_*() methods.

        :param qo_value: the value of the Query object field it's handling
        :param qo_value: Any

        :rtype: _MongoQueryStatementBase
        :raises InvalidRelationError
        :raises InvalidColumnError
        :raises InvalidQueryError
        """
        self.input = qo_value  # no copying. Try not to modify it.
        return self

    # These methods implement the logic of individual statements
    # Note that not all methods are going to be implemented!

    def compile_columns(self):
        """ Compile a list of columns.

        Purpose: argument for Query(*)

        :rtype: list[sqlalchemy.sql.schema.Column]
        """
        raise NotImplementedError()

    def compile_options(self, as_relation):
        """ Compile a list of options for a Query

        Purpose: argument for Query.options(*)

        :param as_relation: Load interface to chain the loader options from
        :type as_relation: sqlalchemy.orm.Load
        :return: list
        """
        raise NotImplementedError()

    def compile_statement(self):
        """ Compile a statement

        :return: SQL statement
        """
        raise NotImplementedError()

    def compile_statements(self):
        """ Compile a list of statements

        :return: list of SQL statements
        """
        raise NotImplementedError()
