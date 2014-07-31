from sqlalchemy import inspect
from sqlalchemy.dialects import postgresql as pg


class _PropertiesBag(object):
    # region Protected

    @staticmethod
    def _is_column_array(col):
        """ Is the column an ARRAY column?

        :type col: sqlalchemy.sql.schema.Column
        :rtype: bool
        """
        return isinstance(col.type, pg.ARRAY)

    @staticmethod
    def _is_column_json(col):
        """ Is the column a JSON column?

        :type col: sqlalchemy.sql.schema.Column
        :rtype: bool
        """
        return isinstance(col.type, pg.JSON)

    @staticmethod
    def _dot_notation(name):
        """ Split a property name that's using dot-notation

        :type name: str
        :rtype: str, list[str]
        """
        path = name.split('.')
        return path[0], path[1:]

    #endregion

    def __contains__(self, name):
        """ Test if the property is in the bag
        :param name: Property name
        :type name: str
        :rtype: bool
        """
        raise NotImplementedError

    def __getitem__(self, name):
        """ Get the property by name
        :param name: Property name
        :type name: str
        :rtype: sqlalchemy.orm.interfaces.MapperProperty
        """
        raise NotImplementedError


class ColumnsBag(_PropertiesBag):
    """ Columns bag """

    def __init__(self, columns):
        """ Init columns
        :param columns: Model columns
        :type columns: dict[sqlalchemy.orm.properties.ColumnProperty]
        """
        self._columns = columns
        self._column_names = set(self._columns.keys())
        self._array_columns = {name: col for name, col in self._columns.items() if self._is_column_array(col)}
        self._json_columns =  {name: col for name, col in self._columns.items() if self._is_column_json(col)}

    def is_column_array(self, name):
        """ Is the column an ARRAY column
        :type name: str
        :rtype: bool
        """
        column_name = self._dot_notation(name)[0]
        return column_name in self._array_columns

    def is_column_json(self, name):
        """ Is the column a JSON column
        :type name: str
        :rtype: bool
        """
        column_name = self._dot_notation(name)[0]
        return column_name in self._json_columns

    @property
    def names(self):
        """ Get the set of column names
        :rtype: set[str]
        """
        return self._column_names

    def items(self):
        """ Get columns
        :rtype: dict[sqlalchemy.orm.properties.ColumnProperty]
        """
        return self._columns.items()

    def __getitem__(self, column_name):
        try:
            return self._columns[column_name]
        except KeyError:
            raise AssertionError('Unknown column: `{}`'.format(column_name))


class PrimaryKeyBag(ColumnsBag):
    """ Primary Key Bag """


class DotColumnsBag(ColumnsBag):
    """ Columns bag with additional capabilities:

        - For JSON fields: field.prop.prop -- dot-notation access to sub-properties
    """

    def __getitem__(self, name):
        column_name, path = self._dot_notation(name)
        col = super(DotColumnsBag, self).__getitem__(column_name)
        # JSON path
        if path and self.is_column_json(column_name):
            col = col[path].astext
        return col


class RelationshipsBag(_PropertiesBag):
    """ Relationships bag with additional capabilities """

    def __init__(self, relationships):
        """ Init relationships
        :param relationships: Model relationships
        :type relationships: dict[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        self._rels = relationships
        self._rel_names = set(self._rels.keys())

    @property
    def names(self):
        """ Get the set of relation names
        :rtype: set[str]
        """
        return self._rel_names

    def items(self):
        """ Get relationships
        :rtype: dict[sqlalchemy.orm.relationships.RelationshipProperty]
        """
        return self._rels.items()

    def __contains__(self, name):
        return name in self._rels

    def __getitem__(self, name):
        try:
            return self._rels[name]
        except KeyError:
            raise AssertionError('Unknown relationship: `{}`'.format(name))


class ModelPropertyBags(object):
    """ Model property bags """

    def __init__(self, model):
        """ Init bags

        :param model: Model
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        """
        ins = inspect(model)
        self.model = model

        #: Column properties
        self.columns   =    DotColumnsBag({name:   getattr(model, name)   for name, c in ins.column_attrs .items()})

        #: Relationship properties
        self.relations = RelationshipsBag({name:   getattr(model, name)   for name, c in ins.relationships.items()})

        #: Primary key properties
        self.pk        =    PrimaryKeyBag({c.name: self.columns[c.name]   for       c in ins.primary_key})

        #: Nullable properties
        self.nullable  =       ColumnsBag({name: c for name, c in self.columns.items() if c.nullable})
