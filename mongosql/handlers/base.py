from ..bag import ModelPropertyBags
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoQueryHandlerBase:
    """ An implementation of a handler from MongoQuery

        Every subclass will handle a single field from the Query object
    """

    #: Name of the QueryObject section that this object is capable of handling
    query_object_section_name = None

    def __init__(self, model, bags):
        """ Initialize the Query Object section handler with a model.

        This method does *not* receive any input data just yet, with the purpose of having an
        object that can be extended with some interesting defaults right at init time.

        :param model: The sqlalchemy model it's being applied to
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        :param bags: Model bags.
            We have to have `bags` provided to us, because then someone may subclass MongoQuery,
            use a different MongoPropertyBags, and customize the way a model is analyzed.
        :type bags: ModelPropertyBags

        NOTE: Any arguments that have default values will be treated as handler settings!!
        """
        #: The model to handle the Query Object for
        self.model = model  # the model, ot its alias (when used with self.aliased())
        #: Model property bags: because we need access to the lists of its properties
        self.bags = bags
        #: A CombinedBag() that allows to handle properties of multiple types (e.g. columns + hybrid properties)
        self.supported_bags = self._get_supported_bags()

        # Has the input() method been called already?
        # This may be important for handlers that depend on other handlers
        self.input_received = False

        # Has the aliased() method been called already?
        # This is important because it can't be done again, or undone.
        self.is_aliased = False

        # Should this handler's alter_query() be skipped by MongoQuery?
        # This is used by MongoJoin when it removes a filtering condition into the ON-clause,
        # and does not want the original filter to be executed.
        self.skip_this_handler = False

        #: MongoQuery bound to this object. It may remain uninitialized.
        self.mongoquery = None

    def with_mongoquery(self, mongoquery):
        """ Bind this object with a MongoQuery

            :type mongoquery: mongosql.query.MongoQuery
            """
        self.mongoquery = mongoquery
        return self

    def __copy__(self):
        """ Some objects may be reused: i.e. their state before input() is called.

        Reusable handlers are implemented using the Reusable() wrapper which performs the
        automatic copying on input() call
        """
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__.update(self.__dict__)
        return result

    def aliased(self, model):
        """ Use an aliased model to build queries

            This is used by MongoQuery.aliased(), which is ultimately useful to MongoJoin handler.
            Note that the method modifies the current object and does not make a copy!
        """
        # Only once
        assert not self.is_aliased, 'You cannot call {}.aliased() ' \
                                    'on a handler that has already been aliased()' \
                                    .format(self.__class__.__name__)
        self.is_aliased = True

        # aliased() everything
        self.model = model
        self.bags = self.bags.aliased(model)
        self.supported_bags = self._get_supported_bags()  # re-initialize
        return self

    def _get_supported_bags(self):
        """ Get the _PropertiesBag interface supported by this handler

        :rtype: mongosql.bag._PropertiesBagBase
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

    def input_prepare_query_object(self, query_object):
        """ Modify the Query Object before it is processed.

        Sometimes a handler would need to alter it.
        Here's its chance.

        This method is called before any input(), or validation, or anything.

        :param query_object: dict
        """
        return query_object

    def input(self, qo_value):
        """ Get a section of the Query object.

        The purpose of this method is to receive the input, validate it, and store as a public
        property so that external tools may export its value.
        Note that validation does not *have* to happen here: it may in fact be implemented in one
        of the compile_*() methods.

        :param qo_value: the value of the Query object field it's handling
        :param qo_value: Any

        :rtype: MongoQueryHandlerBase
        :raises InvalidRelationError
        :raises InvalidColumnError
        :raises InvalidQueryError
        """
        self.input_value = qo_value  # no copying. Try not to modify it.

        # Set the flag
        self.input_received = True

        # Make sure that input() can only be used once
        self.input = self.__raise_input_not_reusable

        return self

    def is_input_empty(self):
        """ Test whether the input value was empty """
        return not self.input_value

    def __raise_input_not_reusable(self, *args, **kwargs):
        raise RuntimeError("You can't use the {}.input() method twice. "
                           "Wrap the class into Reusable(), or copy() it!"
                           .format(self.__class__.__name__))

    # These methods implement the logic of individual handlers
    # Note that not all methods are going to be implemented by subclasses!

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

    def alter_query(self, query, as_relation):
        """ Alter the given query and apply the Query Object section this handler is handling

        :param query: The query to apply this MongoSQL Query Object to
        :type query: Query
        :param as_relation: Load interface to work with nested relations.
            Note that some classed need it, others don't
        :type as_relation: Load
        :rtype: Query
        """
        raise NotImplementedError()

    def get_final_input_value(self):
        """ Get the final input of the handler """
        return self.input_value
