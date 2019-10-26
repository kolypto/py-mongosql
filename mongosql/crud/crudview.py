from enum import Enum
from functools import reduce

from ..util.method_decorator import method_decorator
from ..util import load_many_instance_dicts, EntityDictWrapper
from ..util import model_primary_key_columns_and_names, entity_dict_has_primary_key

from ..query import MongoQuery
from ..util.history_proxy import ModelHistoryProxy
from .crudhelper import CrudHelper, StrictCrudHelper

from typing import Iterable, Mapping, Set, Union, Tuple, Callable, List
from sqlalchemy.orm import Query, Session, object_session


class CRUD_METHOD(Enum):
    """ CRUD method """
    GET = 'GET'
    LIST = 'LIST'
    CREATE = 'CREATE'
    UPDATE = 'UPDATE'
    DELETE = 'DELETE'


class CrudViewMixin:
    """ A mixin class for implementations of CRUD views.

        This class is supposed to be re-initialized for every request.

        To implement a CRUD view:
        1. Implement some method to extract the Query Object from the request
        2. Set `crudhelper` at the class level, initialize it with the proper settings
        3. Implement the `_get_db_session()` and the `_get_query_object()` methods
        4. If necessary, implement the `_save_hook()` to customize new & updated entities
        5. Override `_method_list()` and `_method_get()` to customize its output
        6. Override `_method_create()`, `_method_update()`, `_method_delete()` and implement saving to the DB
        7. Use [`@saves_relations`](#saves_relationsfield_names) method decorator to handle custom fields in the input dict

        For an example on how to use CrudViewMixin, see this implementation:
        [tests/crud_view.py](tests/crud_view.py)

        Attrs:
            _mongoquery (MongoQuery):
                The MongoQuery object used to process this query.
    """

    #: Set the CRUD helper object at the class level
    crudhelper = None  # type: Union[CrudHelper, StrictCrudHelper]

    #: List of columns and relationships that must be loaded with MongoQuery.ensure_loaded()
    #: Note that you can also use related columns: "relation.col_name" to ensure it's loaded (join-project)
    #: Remember that every time you use ensure_loaded() on a relationship, you disable filtering for it!
    ensure_loaded = ()

    def __init__(self):
        #: The MongoQuery for this request, if it was indeed initialized by _mquery()
        self.__mongoquery = None  # type: MongoQuery

        #: The current CRUD method
        self._current_crud_method = None

    def __init_subclass__(cls, **kwargs):
        #: The list of all `@saves_relations()` fields
        cls._saves_relations_names = saves_relations.all_relation_names_from(cls)

    # region Abstract Methods

    def _get_db_session(self) -> Session:
        """ (Abstract method) Get a DB session to be used for queries made in this view

        :return: sqlalchemy.orm.Session
        """
        raise NotImplementedError('_get_db_session() not implemented on {}'
                                  .format(type(self)))

    def _get_query_object(self) -> Mapping:
        """ (Abstract method) Get the Query Object for the current query.

            Note that the Query Object is not only supported for get() and list() methods, but also for
            create(), update(), and delete(). This enables the API use to request a relationship right away.
        """
        raise NotImplementedError

    # endregion

    # region Hooks

    def _mongoquery_hook(self, mongoquery: MongoQuery) -> MongoQuery:
        """ (Hook) A hook invoked in _mquery() to modify MongoQuery, if necessary

            This is the last chance to modify a MongoQuery.
            Right after this hook, it end()s, and generates an sqlalchemy Query.

            Use self._current_crud_method to tell what is going on: create, read, update, delete?
        """
        return mongoquery

    def _save_hook(self, new: object, prev: object = None):
        """ (Hook) Hooks into create(), update() methods, before an entity is saved.

            This allows to make some changes to the instance before it's actually saved.
            The hook is provided with both the old and the new versions of the instance (!).

            Note that it is executed before flush(), so DB defaults are not available yet.

            :param new: The new instance
            :param prev: Previously persisted version (is provided only when updating).
        """
        pass

    # NOTE: there's no delete hook. Override _method_delete() to implement it.

    # endregion

    # ###
    # CRUD methods' implementations

    def _method_get(self, *filter, **filter_by) -> object:
        """ (CRUD method) Fetch a single entity: as in READ, single entity

            Normally, used when the user has supplied a primary key:

                GET /users/1

            :param query_obj: Query Object
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        self._current_crud_method = CRUD_METHOD.GET
        instance = self._get_one(self._get_query_object(), *filter, **filter_by)
        return instance

    def _method_list(self, *filter, **filter_by) -> Iterable[object]:
        """ (CRUD method) Fetch a list of entities: as in READ, list of entities

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
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        self._current_crud_method = CRUD_METHOD.LIST

        # Query
        query = self._mquery(self._get_query_object(), *filter, **filter_by)

        # Done
        return self._method_list_result_handler(query)

    def _method_list_result_handler(self, query: Query) -> Union[int, Iterable[object], Iterable[Tuple]]:
        """ Handle the results from method_list() """
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

    def _method_list_result__entities(self, entities: Iterable[object]) -> Iterable[object]:
        """ Handle _method_list() result when it's a list of entities """
        return list(entities)  # because it may be an iterable

    def _method_list_result__groups(self, dicts: Iterable[dict]) -> Iterable[dict]:
        """ Handle _method_list() result when it's a list of dicts: the one you get from GROUP BY """
        return dicts

    def _method_list_result__count(self, n: int) -> int:
        """ Handle _method_list() result when it's an integer number: the one you get from COUNT() """
        return n

    def _method_create(self, entity_dict: dict) -> object:
        """ (CRUD method) Create a new entity: as in CREATE

            Normally, used when the user has supplied no primary key:

                POST /users/
                {'name': 'Hakon'}

            :param entity_dict: Entity dict
            :return: The created instance (to be saved)
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        self._current_crud_method = CRUD_METHOD.CREATE

        # Create a new instance
        # (wrapped with a relationship saver)
        instance = self._handle_saving_relationships(
            entity_dict,
            None,
            lambda entity_dict: self.crudhelper.create_model(entity_dict)
        )

        # Run the hook
        self._save_hook(instance, None)

        # Done
        # We don't save anything here
        return instance

    def _method_update(self, entity_dict: dict, *filter, **filter_by) -> object:
        """ (CRUD method) Update an existing entity by merging the fields: as in UPDATE

            Normally, used when the user has supplied a primary key:

                POST /users/1
                {'id': 1, 'name': 'Hakon'}

            :param entity_dict: Entity dict
            :param filter: Criteria to find the previous entity
            :param filter_by: Criteria to find the previous entity
            :return: The updated instance (to be saved)
            :raises sqlalchemy.orm.exc.NoResultFound: The entity not found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple entities found with the filter condition
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        self._current_crud_method = CRUD_METHOD.UPDATE

        # Load the instance
        if isinstance(entity_dict, EntityDictWrapper):
            # _method_create_or_update_many() feeds us with pre-loaded objects. Use if available.
            # This gives a nice performance boost because then we make only one query per object
            instance = entity_dict.loaded_instance
        else:
            instance = self._get_one(self._get_query_object(), *filter, **filter_by)

        # Old instance: is used to provide the _save_hook() with the previous state of the instance
        old_instance = ModelHistoryProxy(instance)

        # Update it
        # (wrapped with a relationship saver)
        instance = self._handle_saving_relationships(
            entity_dict,
            old_instance,
            lambda entity_dict: self.crudhelper.update_model(entity_dict, instance)
        )

        # Run the hook
        self._save_hook(instance, old_instance)

        # Done
        # We don't save anything here
        return instance

    def _method_delete(self, *filter, **filter_by) -> object:
        """ (CRUD method) Delete an existing entity: as in DELETE

            Normally, used when the user has supplied a primary key:

                DELETE /users/1

            Note that it will load the entity from the database prior to deletion.

            :param filter: Criteria to find the previous entity
            :param filter_by: Criteria to find the previous entity
            :return: The instance to be deleted
            :raises sqlalchemy.orm.exc.NoResultFound: The entity not found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple entities found with the filter condition
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        self._current_crud_method = CRUD_METHOD.DELETE

        # Load
        instance = self._get_one(self._get_query_object(), *filter, **filter_by)

        # Return
        # We don't delete anything here
        return instance

    # CRUD methods for bulk operations

    def _method_create_or_update(self, entity_dict: dict, *filter, **filter_by) -> object:
        """ (CRUD method) Create-or-update (aka upsert): create if no PK is given, update if PK is given

        Is normally used when the primary key may or may not be supplied inside the entity dict.

        Note that this method is sub-optimal when used on many objects because it will make one SELECT query per object.
        """
        # Determine whether the PK is provided: all Primary Key columns must be present in the entity dict
        pk_provided = entity_dict_has_primary_key(self.crudhelper.bags.pk.names, entity_dict)

        # Update or Create?
        if pk_provided:
            return self._method_update(entity_dict, *filter, **filter_by)
        else:
            return self._method_create(entity_dict)

    def _method_create_or_update_many(self,
                                      entity_dicts: Iterable[dict],
                                      *filter, **filter_by) -> Iterable[EntityDictWrapper]:
        """ (CRUD method) Create-or-update many objects (aka upsert): create if no PK, update with PK

        This smart method can be used to save (upsert: insert & update) many objects at once.

        It will *load* those objects that have primary key fields set and update them with _method_update().
        It will *create* objects that do not have primary key fields with _method_create()
        It will *delegate* to _method_create_or_update_many__create_arbitrary_pk() that have primary key fields
        but were not found in the database.

        Note that the method uses EntityDictWrapper to preserve the order of entity dicts
        and return results associated with them:

        * EntityDictWrapper.instance is the resulting instance to be saved
        * EntityDictWrapper.error is the exception (if any). It's not raised! Raise it if you will.

        Note that you may wrap entity dicts with EntityDictWrapper yourself.
        In this case, you may:

        * set EntityDictWrapper.skip = True to cause the method to ignore it completely
        """
        # Process these entity dicts, load those that have primary keys set
        pk_columns, pk_names = model_primary_key_columns_and_names(self.crudhelper.model)
        wrapped_entity_dicts = load_many_instance_dicts(
            pk_columns=pk_columns,
            query=self._mquery_simple(self._get_query_object(), *filter, **filter_by).options(no_limit_offset=True).end(),
            entity_dicts=EntityDictWrapper.from_entity_dicts(self.crudhelper.model, entity_dicts, pk_names=pk_names)
        )

        # Create some, update others
        # The update handler is patched to use EntityDictWrappers and the instance object that's available within
        for wrapped_entity_dict in wrapped_entity_dicts:
            # Skip objects marked for skipping
            if wrapped_entity_dict.skip:
                continue

            # Create, Update, Delegate
            try:
                # is_new: create (no pk provided)
                if wrapped_entity_dict.is_new:
                    instance = self._method_create(wrapped_entity_dict)
                # is_found: update (pk provided & is found in the DB)
                elif wrapped_entity_dict.is_found:
                    # It's safe to update because the primary key has 100% match with the submitted entity dict.
                    # Otherwise, it wouldn't have been found by this very primary key, right? ;)
                    instance = self._method_update(wrapped_entity_dict)
                # is_not_found: custom handler (pk provided & is not found in the DB)
                elif wrapped_entity_dict.is_not_found:
                    # Delegate to the custom handler
                    instance = self._method_create_or_update_many__create_arbitrary_pk(wrapped_entity_dict)
                else:
                    raise RuntimeError('How did we get here?')
            except BaseException as e:
                # Uncomment to debug errors raised while processing individual objects
                #raise

                # Collect every exception into the `error` field
                wrapped_entity_dict.error = e

                # Undo any possible changes done to the instance
                if wrapped_entity_dict.loaded_instance:
                    # Get the session and expire the object. This will undo all changes
                    ssn: Session = object_session(wrapped_entity_dict.loaded_instance)
                    ssn.expire(wrapped_entity_dict.loaded_instance)
                    ssn.expunge(wrapped_entity_dict.loaded_instance)
            else:
                # Put it into `instance`
                wrapped_entity_dict.instance = instance

        # Done
        return wrapped_entity_dicts

    def _method_create_or_update_many__create_arbitrary_pk(self, entity_dict: EntityDictWrapper) -> object:
        """ Custom handler for that particular case when the user has submitted an object with a primary key that's not found in the DB

        In general, it's not safe to let the user choose arbitrary primary keys,
        so the best strategy is to either ignore such values (the default) or raise errors.

        However, in some cases it may be desirable to create such objects (e.g. when natural primary keys are used).
        Whatever logic best applies to your views, implement it here.

        Returns:
            The created instance, or None if none should be created; or perhaps, raise an error?
        """
        return None

    # region Helpers

    def _query(self) -> Query:
        """ Make the initial Query object to work with """
        return self._get_db_session().query(self.crudhelper.model)

    @property
    def _mongoquery(self) -> MongoQuery:
        """ Get the current MongoQuery for this request, or initialize a new one.

        :rtype: MongoQuery
        """
        # Init a new one, if necessary
        if not self.__mongoquery:
            # MongoQuery object is not explicitly created during CREATE requests
            # Therefore, we have to initialize it manually
            self.__mongoquery = self._mquery_simple(self._get_query_object())

        # Return
        return self.__mongoquery

    @_mongoquery.setter
    def _mongoquery(self, mongoquery: MongoQuery):
        self.__mongoquery = mongoquery

    def _mquery_end(self, mongoquery: MongoQuery) -> Query:
        """ Finalize a MongoQuery and generate a Query """
        return mongoquery.end()

    def _mquery(self, query_object: Mapping = None, *filter, **filter_by) -> Query:
        """ Run a MongoQuery and invoke the View's hooks.

            This method is used by other methods to initialize all CRUD queries in this view.

            :param query_object: Query Object
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # Initialize the MongoQuery
        mquery = self._mquery_simple(query_object, *filter, **filter_by)

        # ensure_loaded(), when applicable
        if mquery.result_contains_entities():
            mquery.ensure_loaded(*self.ensure_loaded)

        # Session
        mquery.with_session(self._get_db_session())  # not really necessary, because _query() does it already

        # MongoQuery hook
        mquery = self._mongoquery_hook(mquery)

        # Store
        self._mongoquery = mquery

        # Query
        q = self._mquery_end(mquery)

        # Done
        return q

    def _mquery_simple(self, query_object: Mapping = None, *filter, **filter_by) -> MongoQuery:
        """ Use a MongoQuery to make a Query, with the Query Object, and initial custom filtering applied.

            This method does not run the View's hooks; that's why it is "simple".
            See _mquery() to everything that's left out.

            :param query_object: Query Object
            :type query_object: dict | None
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :raises exc.InvalidQueryError: Query Object errors made by the user
        """
        # We have to make a Query object and filter it in advance,
        # because later on, MongoSQL may put a LIMIT, or something worse, and no filter() will be possible anymore.
        q = self._query()

        # Filters: only apply when necessary
        if filter:
            q = q.filter(*filter)
        if filter_by:
            q = q.filter_by(**filter_by)

        # MongoQuery
        mquery = self.crudhelper.query_model(query_object, from_query=q)  # type: MongoQuery

        # Done
        return mquery

    def _get_one(self, query_obj: Mapping, *filter, **filter_by) -> object:
        """ Utility method that fetches a single entity.

            You will probably want to override it with custom error handling

            :param query_obj: Query Object
            :param filter: Additional filter() criteria
            :param filter_by: Additional filter_by() criteria
            :raises exc.InvalidQueryError: Query Object errors made by the user
            :raises sqlalchemy.orm.exc.NoResultFound: Nothing found
            :raises sqlalchemy.orm.exc.MultipleResultsFound: Multiple found
        """
        # Query
        query = self._mquery(query_obj, *filter, **filter_by)

        # Result
        return query.one()

    def _handle_saving_relationships(self, entity_dict: dict, prev_instance: object, wrapped_method: Callable) -> object:
        """ A helper wrapper that will save relationships of an instance while it's being created or updated.

            Every method decorated with @saves_relations handles one relationship.
            The idea is that this method will pluck `self.saves_relations` relatioships from the entity_dict,
            and then pass them to the `_save_relations()` handler.

            Args:
                entity_dict:
                    A dict that has come from the user that's going to save an entity
                prev_instance:
                    The previous version of the instance (if available)
                wrapped_method:
                    The method wrapped with this helper.
        """
        # Pluck relations out of the entity dict
        relations_to_be_saved = {k: entity_dict.pop(k)
                                 for k in self._saves_relations_names
                                 if k in entity_dict}

        # Update it
        new_instance = wrapped_method(entity_dict)

        # Save relations
        saves_relations.execute_handler_methods(
            self, relations_to_be_saved,
            new_instance, prev_instance
        )

        # Done
        return new_instance

    # endregion


class saves_relations(method_decorator):
    """ A decorator that marks a method that handles saving some related models (or any other custom values)

        Whenever a relationship is marked for saving with the help of this decorator,
        it is plucked out of the incoming JSON dict, and after an entity is created,
        it is passed to the method that this decorator decorates.

        In addition to saving relationships, a decorated mthod can be used to save any custom properties:
        they're plucked out of the incoming entity dict, and handled manually anyway.
        Note that all attributes that do not exist on the model are plucked out, and the only way to handle them
        is through this method.

        NOTE: this method is executed before _save_hook() is.

        Example usage:

        ```python
        from mongosql import saves_relations
        from mongosql import ABSENT  # unique marker used to detect values not provided

        class UserView(CrudViewMixin):
            @saves_relations('articles')
            def save_articles(self, new: object, prev: object = None, articles = ABSENT):
                if articles is not ABSENT:
                    ...  # articles-saving logic
        ```

        NOTE: the handler method is called with two positional arguments, and the rest being keyword arguments:

            save_articles(new_instance, prev_instance, **relations_to_be_saved)

        NOTE: If the user did not submit any related entity, the method is still called, with relationship argument = None.

        Multiple relations can be provided: in this case, all of them are handled with one method.
    """
    METHOD_PROPERTY_NAME = 'saves_relations'

    def __init__(self, *field_names: Iterable[str]):
        assert isinstance(field_names, Iterable), "No `field_name` provided to @saves_relations which requires an argument"
        self.field_names = field_names

        super().__init__()

    @classmethod
    def all_relation_names_from(cls, View: type) -> Set[str]:
        """ Go through all @saves_relations and collect the field names that they handle """
        return reduce(lambda acc, d: acc.update(d.field_names) or acc,
                      cls.all_decorators_from(View),
                      set())

    @classmethod
    def execute_handler_methods(cls, view: object, input_data: Mapping, *decorator_args):
        """ Given the input data, execute all decorated methods.

            It will go through all the decorated methods that you've provided (for performance reasons)

        """
        View = view.__class__
        for decorator in cls.all_decorators_from(View):
            # Get the kwargs: the relationships (or whatever fields)
            decorator_kwargs = {name: input_data.get(name, None)
                                for name in decorator.field_names
                                if name in input_data}
            
            # Call it -- even if no kwargs were provided
            decorator.method(view, *decorator_args, **decorator_kwargs)


class _ABSENT_TYPE:
    """ A falsy marker to be used for arguments not provided to a function """
    def __repr__(self):
        return '-'
    def __bool__(self):
        return False


ABSENT = _ABSENT_TYPE()  # A falsy marker to be used for @saves_relations

