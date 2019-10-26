""" Tools for working with bulk operations """
from typing import Iterable, List, Tuple, Union, Mapping, Sequence
from collections import UserDict, UserList

from sqlalchemy import inspect, Column, tuple_ as sql_tuple
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Query
from sqlalchemy.sql.elements import BinaryExpression


NoneType = type(None)


class EntityDictWrapper(UserDict, dict):  # `dict` to allow isinstance() checks
    """ Entity dict wrapper with metadata

    When the user submits N objects to be saved, we need to handle extra information along with every dict.
    This is what this object is for: for caching primary keys and associating results with the input

    Attributes:
        Model: the model class
        has_primary_key: Whether the entity dict contains the complete primary key
        primary_key_tuple: The primary key tuple, if present
        ordinal_number: The ordinal number of this entity dict in the submitted data
        skip: Ignore this entity dict
        loaded_instance: The instance that was (possibly) loaded from the database (if the primary key was given)
        instance: The instance that was (possibly) saved as a result of this operation (unless an error has occurred)
        error: The exception that was (possibly) raised while processing this entity dict (if an error has occurred)
    """
    Model: DeclarativeMeta
    has_primary_key: bool
    primary_key_tuple: Union[NoneType, Tuple]

    ordinal_number: int = None
    skip: bool = False

    loaded_instance: object = None
    instance: object = None
    error: BaseException = None

    def __init__(self,
                 Model: DeclarativeMeta,
                 entity_dict: dict,
                 *,
                 ordinal_number: int = None,
                 pk_names: Sequence[str] = None):
        super().__init__(entity_dict)
        self.Model = Model
        self.ordinal_number = ordinal_number

        # Primary key names: use the provided list; get it ourselves if not provided
        if not pk_names:
            _, pk_names = model_primary_key_columns_and_names(Model)

        # The primary key tuple
        try:
            self.primary_key_tuple = tuple(entity_dict[pk_field]
                                           for pk_field in pk_names)
            self.has_primary_key = True
        # If any of the primary key fields has raised a KeyError, assume that no PK is defined
        except KeyError:
            self.has_primary_key = False
            self.primary_key_tuple = None

    @classmethod
    def from_entity_dicts(cls,
                          Model: DeclarativeMeta,
                          entity_dicts: Sequence[dict],
                          *,
                          preprocess: callable = None,
                          pk_names: Sequence[str] = None) -> Sequence['EntityDictWrapper']:
        """ Given a list of entity dicts, create a list of EntityDictWrappers with ordinal numbers

        If any dicts are already wrapped with EntityDictWrapper, it's not re-wrapped;
        but be careful to maintain their ordinal numbers, or the client will have difficulties!

        Example:

            _, pk_names = model_primary_key_columns_and_names(Model)
            entity_dicts = EntityDictWrapper.from_entity_dicts(models.User, [
                {'id': 1, 'login': 'kolypto'},
                {         'login': 'vdmit11'},
            ], pk_names=pk_names)
        """
        # Prepare the list of primary key columns
        if not pk_names:
            _, pk_names = model_primary_key_columns_and_names(Model)

        # Generator: EntityDictWrappers with ordinal numbers
        return [entity_dict
                if isinstance(entity_dict, EntityDictWrapper) else
                cls(Model, entity_dict, ordinal_number=i, pk_names=pk_names)
                for i, entity_dict in enumerate(entity_dicts)]

    # Object states

    @property
    def is_new(self):
        """ The submitted object has no primary key and will therefore be created """
        return not self.has_primary_key

    @property
    def is_found(self):
        """ The submitted object has a primary key and it was successfully loaded from the database """
        return self.has_primary_key and self.loaded_instance is not None

    @property
    def is_not_found(self):
        """ The submitted object has a primary key but it was not found in the database """
        return self.has_primary_key and self.loaded_instance is None


# This function isn't really used by MongoSQL, but it's here because it's beautiful
# MongoSQL uses load_many_instance_dicts() instead
def filter_many_objects_by_list_of_primary_keys(Model: DeclarativeMeta, entity_dicts: Sequence[dict]) -> BinaryExpression:
    """ Build an expression to load many objects from the database by their primary keys

    This function uses SQL tuples to build an expression which looks like this:

        SELECT * FROM users WHERE (uid, login) IN ((1, 'vdmit11'), (2, 'kolypto'));

    Example:

        entity_dicts = [
            {'id': 1, ...},
            {'id': 2, ...},
            ...
        ]
        ssn.query(models.User).filter(
            filter_many_objects_by_list_of_primary_keys(models.User, entity_dicts)
        )

    Args:
        Model: the model to query
        entity_dicts: list of entity dicts to pluck the PK values from

    Returns:
        The condition for filter()

    Raises:
        KeyError: one of `entity_dicts` did not contain a full primary key set of fields
    """
    pk_columns, pk_names = model_primary_key_columns_and_names(Model)

    # Build the condition: (primary-key-tuple) IN (....)
    # It uses sql tuples and the IN operator: (pk_col_a, pk_col_b, ...) IN ((val1, val2, ...), (val3, val4, ...), ...)
    # Thanks @vdmit11 for this beautiful approach!
    return sql_tuple(*pk_columns).in_(
        # Every object is represented by its primary key tuple
        tuple(entity_dict[pk_field] for pk_field in pk_names)
        for entity_dict in entity_dicts
    )


def load_many_instance_dicts(query: Query, pk_columns: Sequence[Column], entity_dicts: Sequence[EntityDictWrapper]) -> Sequence[EntityDictWrapper]:
    """ Given a list of wrapped entity dicts submitted by the client, load some of them from the database

    As the client submits a list of entity dicts, some of them may contain the primary key.
    This function loads them from the database with one query and returns a list of  EntityDictWrapper objects.

    Note that there will be three kinds of EntityDictWrapper objects: is_new, is_found, is_not_found:

    1. New: entity dicts without a primary key
    2. Found: entity dicts with a primary key that were also found in the database
    3. Not found: entity dicts with a primary key that were not found in the database

    NOTE: no errors are raised for instances that were not found by their primary key!

    Args:
        query: The query to load the instances with
        pk_columns: The list of primary key columns for the target model.
            Use model_primary_key_columns_and_names()
        entity_dicts: The list of entity dicts submitted by the user
    """
    # Load all instances by their primary keys at once
    # It uses sql tuples and the IN operator: (pk_col_a, pk_col_b, ...) IN ((val1, val2, ...), (val3, val4, ...), ...)
    # Thanks @vdmit11 for this beautiful approach!
    instances = query.filter(sql_tuple(*pk_columns).in_(
        # Search by PK tuples
        entity_dict.primary_key_tuple
        for entity_dict in entity_dicts
        if entity_dict.has_primary_key
    ))

    # Prepare a PK lookup object: we want to look up entity dicts by primary key tuples
    entity_dict_lookup_by_pk: Mapping[Tuple, EntityDictWrapper] = {
        entity_dict.primary_key_tuple: entity_dict
        for entity_dict in entity_dicts
        if entity_dict.has_primary_key
    }

    # Match instances with entity dicts
    for instance in instances:
        # Lookup an entity dict by its primary key tuple
        # We safely expect it to be there because objects were loaded by those primary keys in the first place :)
        entity_dict = entity_dict_lookup_by_pk[inspect(instance).identity]
        # Associate the instance with it
        entity_dict.loaded_instance = instance

    # Done
    return entity_dicts


def model_primary_key_columns_and_names(Model: DeclarativeMeta) -> (Sequence[Column], List[str]):
    """ Get the list of primary columns and their names as two separate tuples

    Example:

        pk_columns, pk_names = model_primary_key_columns_and_names(models.User)
        pk_columns  # -> (models.User.id, )
        pk_names  # -> ('id', )
    """
    pk_columns: Sequence[Column] = inspect(Model).primary_key
    pk_names: List[str] = [col.key for col in pk_columns]
    return pk_columns, pk_names

def entity_dict_has_primary_key(pk_names: Sequence[str], entity_dict: dict) -> bool:
    """ Check whether the given dict contains all primary key fields """
    return set(pk_names) <= set(entity_dict)
