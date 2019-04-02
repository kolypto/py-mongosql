from sqlalchemy.orm import Query

from ..query import MongoQuery
from ..util.history_proxy import ModelHistoryProxy
from .crudhelper import CrudHelper, StrictCrudHelper


# CRUD method constants
class CRUD_METHOD:  # TODO: Make it an enum in Python 3
    GET = 'GET'
    LIST = 'LIST'
    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'


class CrudViewMixin(object):
    """ Base class for implementations of CRUD views. This class is supposed to be re-initialized for every request.

        To implement a CRUD view:
        1. Implement some method to extract the Query Object from the request
        2. Set `crudhelper` at the class level, initialize it with the proper settings
        3. Implement the _get_db_session() method
        4. If necessary, implement the _save_hook() to customize new & updated entities
        5. Override _method_list() and _method_get() to customize its output
        6. Override _method_create(), _method_update(), _method_delete() and implement saving to the DB

        For an example on how to use CrudViewMixin, see this implementation:

            tests/crud_view.py
    """

    #: Set the CRUD helper object at the class level
    crudhelper = None  # type: Union[CrudHelper, StrictCrudHelper]

    #: List of columns and relationships that must be loaded with MongoQuery.ensure_loaded()
    ensure_loaded = ()

    def __init__(self):
        #: The MongoQuery for this request
        self._mongoquery = None  # type: MongoQuery

    def _get_db_session(self):
        """ Get a DB session to be used for queries made in this view

        :return: sqlalchemy.orm.Session
        """
        raise NotImplementedError('_get_db_session() not implemented on {}'
                                  .format(type(self)))

    def _mongoquery_hook(self, mongoquery, crud_method):
        """ A hook invoked in _mquery() to modify MongoQuery, if necessary

            This is the last chance to modify a MongoQuery.
            Right after this hook, it end()s, and generates an sqlalchemy Query.

            :type mongoquery: MongoQuery
            :param crud_method: The view method being used. One of the CRUD_METHOD.* constants
            :rtype: MongoQuery
        """
        return mongoquery

    def _save_hook(self, new, prev=None):
        """ Hook into create(), update() methods, before an entity is saved.

            This allows to make some changes to the instance before it's actually saved.
            The hook is provided with both the old and the new versions of the instance (!).

            :param new: The new instance
            :type new: DeclarativeMeta
            :param prev: Previously persisted version (is provided only when updating).
            :type prev: ModelHistoryProxy | None
        """
        pass

    # NOTE: there's no delete hook. Override _method_delete() to implement it.

    # ###
    # CRUD methods' implementations

    def _method_get(self, query_obj=None, *filter, **filter_by):
        """ Fetch a single entity: as in READ, single entity

            Normally, used when the user has supplied a primary key:

                GET /users/1

            :param query_obj: Query Object
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :rtype: DeclarativeMeta
            :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        instance = self._get_one(CRUD_METHOD.GET, query_obj, *filter, **filter_by)
        return instance

    def _method_list(self, query_obj=None, *filter, **filter_by):
        """ Fetch a list of entities: as in READ, list of entities

            Normally, used when the user has supplied no primary key:

                GET /users/

            NOTE: Be careful! This methods does not always return a list of entities!
            It can actually return:
            1. A scalar value: in case of a 'count' query
            2. A list of dicts: in case of an 'aggregate' or a 'group' query
            3. A list or entities: otherwise

            Please use the following MongoQuery methods to tell what's going on:
            MongoQuery.result_contains_entities(), MongoQuery.result_is_scalar(), MongoQuery.result_is_tuples()

            Or, else, override the following sub-methods:
            _method_list_result__entities(), _method_list_result__groups(), _method_list_result__count()

            :param query_obj: Query Object
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :rtype: int | Iterable[DeclarativeMeta]
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # Query
        query = self._mquery(CRUD_METHOD.LIST, query_obj, *filter, **filter_by)

        # Handle: Query Object has count
        if self._mongoquery.result_is_scalar():
            return self._method_list_result__count(query.scalar())

        # Handle: Query Object has group_by and yields tuples
        if self._mongoquery.result_is_tuples():
            # zip() column names together with the values,
            # and make it into a dict
            return self._method_list_result__groups(
                dict(zip(row.keys(), row))
                for row in query)  # return a generator

        # Regular result: entities
        return self._method_list_result__entities(iter(query))  # Return an iterable that yields entities, not a list

    def _method_list_result__entities(self, entities):
        """ Handle _method_list() result when it's a list of entities

            :type entities: Iterable[DeclarativeMeta]
            :rtype: Iterable[DeclarativeMeta]
        """
        return entities

    def _method_list_result__groups(self, dicts):
        """ Handle _method_list() result when it's a list of dicts: the one you get from GROUP BY

            :type dicts: Iterable[dict]
            :type: Iterable[dict]
        """
        return dicts

    def _method_list_result__count(self, n):
        """ Handle _method_list() result when it's an integer number: the one you get from COUNT()

            :type n: int
            :rtype: int
        """
        return n

    def _method_create(self, entity_dict):
        """ Create a new entity: as in CREATE

            Normally, used when the user has supplied no primary key:

                POST /users/
                {'name': 'Hakon'}

            :param entity_dict: Entity dict
            :type entity_dict: dict
            :return: The created instance (to be saved)
            :rtype: DeclarativeMeta
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # Create a new instance
        instance = self.crudhelper.create_model(entity_dict)

        # Run the hook
        self._save_hook(instance, None)

        # Done
        # We don't save anything here
        return instance

    def _method_update(self, entity_dict, *filter, **filter_by):
        """ Update an existing entity by merging the fields: as in UPDATE

            Normally, used when the user has supplied a primary key:

                POST /users/1
                {'id': 1, 'name': 'Hakon'}

            :param entity_dict: Entity dict
            :type entity_dict: dict
            :param filter: Criteria to find the previous entity
            :param filter_by: Criteria to find the previous entity
            :return: The updated instance (to be saved)
            :rtype: DeclarativeMeta
            :raises sqlalchemy.orm.exc.NoResultFound: The entity not found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple entities found with the filter condition
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # Load the instance
        instance = self._get_one(CRUD_METHOD.UPDATE, None, *filter, **filter_by)

        # Update it
        instance = self.crudhelper.update_model(entity_dict, instance)

        # Run the hook
        self._save_hook(
            instance,
            ModelHistoryProxy(instance)
        )

        # Done
        # We don't save anything here
        return instance

    def _method_delete(self, *filter, **filter_by):
        """ Delete an existing entity: as in DELETE

            Normally, used when the user has supplied a primary key:

                DELETE /users/1

            Note that it will load the entity from the database prior to deletion.

            :param filter: Criteria to find the previous entity
            :param filter_by: Criteria to find the previous entity
            :return: The instance to be deleted
            :rtype: DeclarativeMeta
            :raises sqlalchemy.orm.exc.NoResultFound: The entity not found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple entities found with the filter condition
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # Load
        instance = self._get_one(CRUD_METHOD.DELETE, None, *filter, **filter_by)

        # Return
        # We don't delete anything here
        return instance

    # region Helpers

    def _mquery(self, crud_method, query_object=None, *filter, **filter_by):
        """ Use a MongoQuery to make a Query, with the Query Object, and initial custom filtering applied.

            This method is used by other methods to initialize all CRUD queries in this view.

            :param crud_method: One of CRUD_METHOD.*
            :param query_object: Query Object
            :type query_object: dict | None
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :rtype: sqlalchemy.orm.Query
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # We have to make a Query object and filter it in advance,
        # because later on, MongoSQL may put a LIMIT, or something worse, and no filter() will be possible anymore.
        q = Query(self.crudhelper.model)

        # Filters
        q = q.filter(*filter).filter_by(**filter_by)

        # MongoQuery
        self._mongoquery = self.crudhelper.query_model(query_object, from_query=q)  # type: MongoQuery

        # ensure_loaded(), when applicable
        if self._mongoquery.result_contains_entities():
            self._mongoquery.ensure_loaded(*self.ensure_loaded)

        # Session
        self._mongoquery.with_session(self._get_db_session())

        # MongoQuery hook
        self._mongoquery = self._mongoquery_hook(self._mongoquery, crud_method)

        # Query
        q = self._mongoquery.end()
        # NOTE: if you want to capture a query string, that's the place to do it.

        # Done
        return q

    def _get_one(self, crud_method, query_obj, *filter, **filter_by):
        """ Utility method that fetches a single entity.

            You will probably want to override it with custom error handling

            :param crud_method: One of CRUD_METHOD.*
            :param query_obj: Query Object
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :rtype: DeclarativeMeta
            :raises exc.InvalidQueryError: Query Object errors made by the user
            :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        """
        # Query
        query = self._mquery(crud_method, query_obj, *filter, **filter_by)

        # Result
        return query.one()

    # endregion
