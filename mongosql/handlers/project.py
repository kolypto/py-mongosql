from __future__ import absolute_import

from sqlalchemy.orm.base import InspectionAttr

from .base import MongoQueryHandlerBase
from ..bag import CombinedBag
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoProject(MongoQueryHandlerBase):
    """ MongoDB projection operator.

        This operator is essentially the one that enables you to choose which fields to select
        from a query.

        Syntax in Python:

        * None: use default (include all)
        * { a: 1, b: 1 } - include only the given fields; exclude all the rest
        * { a: 0, b: 0 } - exclude the given fields; include all the rest
        * [ a, b, c ] - include only the given fields

        Supports: Columns, Properties, Hybrid Properties, Relationships

        Note: if you want a projection that ONLY supports columns and does not handle properties
        at all, you've got two options:
        1. Specify model properties as `force_exclude`, and they will always be excluded
        2. Subclass, and override _get_supported_bags() with `return self.bags.columns`.
            Then, input() will validate the input against columns only, disallowing properties

        Other useful methods:
        * get_full_projection() will compile a full projection: a projection that contains every
            column of a model, mapped to 1 or 0, depending on whether the user wanted it.
        * __contains__() will test whether a column was requested by this projection operator:
            p = MongoProject(Article).input(...)
            if 'title' in p: ...
    """

    query_object_section_name = 'project'

    def __init__(self, model, default_projection=None,
                 default_exclude=None,
                 force_include=None, force_exclude=None,
                 raiseload=False):
        """ Init projection

        :param model: SQLalchemy model
        :param default_projection: The default projection to use in the absence of any value
        :param default_exclude: A list of column names that are excluded even in exclusion mode.
            You can only get these properties if you request them explicitly.
            This only affects projections in exclusion mode: when the user has specified
            something like {id: 0, text: 0}, and the query would include a lot of fields,
            but you still want some of them removed by default.
            Use this for properties that contain a lot of data.
        :param force_include: A list of column names to include into the output always
        :param force_exclude: A list of column names to exclude from the output always
        :param raiseload: Install a raiseload_col() option on all fields excluded by projection.
            This is a performance safeguard: when your custom code uses certain fields, but a
            projection has excluded them, the situation will result in a LOT of extra queries!
            Solution: `raiseload=True` will raise an exception every time a deferred loading occurs;
            Make sure you manually do `.options(undefer())` on all the columns you need.
        """
        super(MongoProject, self).__init__(model)

        self.default_projection = default_projection or None
        self.default_exclude = set(default_exclude) if default_exclude else None
        self.force_include = set(force_include) if force_include else None
        self.force_exclude = set(force_exclude) if force_exclude else None
        self.raiseload = raiseload

        # On input
        #: Projection mode: self.MODE_INCLUDE, self.MODE_EXCLUDE, self.MODE_MIXED
        self.mode = None
        #: Normalized projection: dict(key=0|1). Not a full projection: some keys may be missing
        self._projection = None
        #: The list of fields that are quietly included
        self.quietly_included = set()

        # Validate
        if self.default_projection:
            # just for the sake of validation
            MongoProject(self.model).input(default_projection)
        if self.default_exclude:
            self.validate_properties(self.default_exclude, where='project:default_exclude')
        if self.force_include:
            self.validate_properties(self.force_include, where='project:force_include')
        if self.force_exclude:
            self.validate_properties(self.force_exclude, where='project:force_exclude')

    def __copy__(self):
        obj = super(MongoProject, self).__copy__()
        obj.quietly_included = obj.quietly_included.copy()
        return obj

    def _get_supported_bags(self):
        return CombinedBag(
            col=self.bags.columns,
            hybrid=self.bags.hybrid_properties,
            prop=self.bags.properties,
        )

    #: MongoSQL projection handler operation modes
    #: Projection handler can operate in three modes
    #: `1`  Inclusion mode: only include the listed columns
    #: `0`  Exclusion mode: exlude the given columns; include everything else
    #: `3`  Mixed mode.
    MODE_INCLUDE = 1
    MODE_EXCLUDE = 0
    MODE_MIXED = 3

    def input(self, projection):
        """ Create a projection

            :type projection: None | Sequence | dict
            :raises InvalidQueryError: invalid input
        """
        super(MongoProject, self).input(projection)

        # Process
        self.mode, self._projection = self._input_process(projection)

        # Settings: default_exclude, force_include, force_exclude
        if self.mode == self.MODE_EXCLUDE and self.default_exclude:
            # Add even more fields that are excluded by default
            # The only way to load them is to explicitly require them.
            self._projection.update({k: 0 for k in self.default_exclude})

        if self.force_include or self.force_exclude:
            self._input_process_force_include_exclude()

        # Done
        assert self.mode is not None  # somebody should have decided by now
        return self

    def _input_process(self, projection):
        """ input(): receive, validate, preprocess """
        # Empty projection
        if not projection:
            projection = (self.default_projection or {}).copy()

        # Array syntax
        if isinstance(projection, (list, tuple)):
            # Convert to dict
            projection = {k: 1 for k in projection}

        # Dict syntax
        if not isinstance(projection, dict):
            raise InvalidQueryError('Projection must be one of: null, array, object; '
                                    '{type} provided'.format(type=type(projection)))

        # Validate keys
        self.validate_properties(projection.keys())

        # Validate values
        values_sum = sum(projection.values())
        if values_sum == 0:
            # all values are 0
            mode = self.MODE_EXCLUDE
        elif values_sum == len(projection):
            # all values are 1
            mode = self.MODE_INCLUDE
        else:
            # The only case when we allow mixing 1-s and 0-s -- is a full projection
            # A full projection includes all fields.
            is_full_projection = set(projection.keys()) == self.supported_bags.names
            if is_full_projection:
                mode = self.MODE_MIXED
            else:
                raise InvalidQueryError('Dict projection values shall be all 0s or all 1s, '
                                        'or a full projection object')

        return mode, projection

    def _input_process_force_include_exclude(self):
        """ input(): process self.force_include and self.force_exclude """
        # force_include
        if self.force_include:
            self.merge(dict.fromkeys(self.force_include, 1))

        # force_exclude
        if self.force_exclude:
            self.merge(dict.fromkeys(self.force_exclude, 0))

    @staticmethod
    def _columns2names(columns):
        """ Convert a list[Column | str] to list[str] names """
        return [c.key
                if isinstance(c, InspectionAttr) else
                c
                for c in columns]

    def include_columns(self, columns):
        """ Include columns into the projection

            Note: you can use column names, or the actual column attributes!
            Make sure you don't use python @property: they don't have a name :(

            :param columns: List of columns, or column names
            :type columns: list[str, sqlalchemy.orm.Column]
        """
        column_names = self._columns2names(columns)
        return self.merge(dict.fromkeys(column_names, 1))

    def exclude_columns(self, columns):
        """ Include columns into the projection

            Note: you can use column names, or the actual column attributes!
            Make sure you don't use python @property: they don't have a name :(

            :param columns: List of columns, or column names
            :type columns: list[str, sqlalchemy.orm.Column]
        """
        column_names = self._columns2names(columns)
        return self.merge(dict.fromkeys(column_names, 0))

    def compile_columns(self):
        """ Get the list of columns to be included into the Query """
        # Note that here we do not iterate over self.supported_bags
        # Instead, we iterate over self.bags.columns, because properties and hybrid properties do
        # not need to be loaded at all!
        if self.mode == self.MODE_INCLUDE or self.mode == self.MODE_MIXED:
            # Only {col: 1}
            return [self.bags.columns[col_name]
                    for col_name, include in self._projection.items()
                    if include == 1 and col_name in self.bags.columns]
        else:
            # Exclude mode
            # All, except {col: 0}
            return [column
                    for col_name, column in self.bags.columns
                    if col_name not in self._projection]

    def compile_options(self, as_relation):
        """ Get the list of load_only() options for a Query.

            Load options are chained from the `as_relation` load interface
        """
        # Short-circuit: empty projection
        # (by default, with input(None), mode=exclude, and project={}
        if self.mode == self.MODE_EXCLUDE and not self._projection:
            return ()  # no restrictions

        # load_only() all those columns
        load_only_columns = self.compile_columns()
        ret = [as_relation.load_only(*load_only_columns)]

        # raiseload_col() on all the rest (if requested)
        if self.raiseload:
            ret.append(as_relation.raiseload_col('*'))

        # done
        return ret

    def compile_option_for_column(self, column_name, as_relation):
        """ Compile the loader option for a single column.

        This method is used by MongoJoin handler to restore some of the columns to their original projection.

        :param column_name: The column to render the option for
        :param as_relation: Load interface
        :return: option
        """
        column = self.bags.columns[column_name]

        # Included in projection: load it
        if column_name in self:
            return as_relation.undefer(column)

        # Not included
        if self.raiseload:
            # raiseload
            return as_relation.raiseload_col(column)
        else:
            # deferred
            return as_relation.defer(column)

    # Not Implemented for this Query Object handler
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation):
        assert as_relation is not None
        return query.options(self.compile_options(as_relation))

    # Extra features

    @property
    def projection(self):
        """ Get the current projection as a dict

            Depending on self.mode, it can be:

            self.mode = MODE_INCLUDE:   all {key: 1}
            self.mode = MODE_EXCLUDE:   all {key: 0}
            self.mode = MODE_MIXED:     mixed {key: 0, key: 1}, but having every key of the model
        """
        proj = self._projection.copy()
        proj.update({k: 0 for k in self.quietly_included})  # force 0s on them
        return proj

    def merge(self, projection, quietly=False):
        """ Merge another projection into the current one.

        This enables you to include or exclude additional columns after the Query Object has been processed.
        For instance, your custom code needs certain fields, and you want to make sure that these are
        loaded, regardless of the QueryObject on the input.

        Note that this method will modify the projection.
        If you want certain fields loaded, but without any trace in the projection object,
        you'll have to manually invoke query.options(undefer(Model.column_name)).

        :param projection: Projection dict
        :type projection: dict | list
        :param quietly: Whether to include the new relations and projections quietly:
            that is, without changing the results of `self.projection` and `self.pluck_instance()`.
            See MongoQuery.merge() for more info.
        :type quietly: bool
        :rtype: MongoProject
        """
        # Validate
        mode, projection = self._input_process(projection)

        # Make a copy because we're going to modify it
        orig_mode = self.mode
        orig_projection = self._projection
        self._projection = self._projection.copy()

        # Now, the logic goes as follows.
        # When the two modes are compatible (mode == self.mode), we can just update() the dict.
        # But when the two modes are incompatible (e.g., one in inclusion mode, and one in exclusion mode),
        # We have to use the full projection object, and update it.
        if self.mode == mode:
            # Compatible modes: just merge
            self._projection.update(projection)
        elif mode == self.MODE_INCLUDE and self.mode == self.MODE_EXCLUDE:
            # merge(include) in self.exclude mode
            # These modes are incompatible. Got to use full projection
            self._projection = self.get_full_projection()
            self._projection.update(projection)
            self.mode = self.MODE_MIXED
        elif mode == self.MODE_EXCLUDE and self.mode == self.MODE_INCLUDE:
            # merge(exclude) in self.include mode: just drop banned keys
            # this is a short-cut
            drop_keys = set(projection.keys()) & set(self._projection.keys())
            for k in drop_keys:
                self._projection.pop(k)
        else:
            raise AssertionError('Unknown combination of self.mode and mode')

        # Quiet mode handler
        if quietly:
            # Only handle cases where more keys were included
            if mode == self.MODE_INCLUDE and orig_mode == self.MODE_INCLUDE:
                # originally INCLUDE, merge INCLUDE
                # More keys included
                new_keys = set(self._projection.keys()) - set(orig_projection.keys())
                self.quietly_included.update(new_keys)
            elif mode == self.MODE_INCLUDE and orig_mode == self.MODE_EXCLUDE:
                # originally EXCLUDE, merged INCLUDE
                # Ended up in MIXED mode because more keys included

                # Mathematics by example:
                # all fields:       a b c d e f
                # prev-excluded:      b c
                # prev-included:    a     d e f
                # merge include:        c d e
                # full projection:  a   c d e f
                # now included:     a   c d e f
                # newly included:       c
                # How to get `newly included`?
                # (now included) intersects with (prev-excluded)
                now_included = set(k for k, v in self._projection.items() if v == 1)
                previously_excluded = set(orig_projection.keys())
                new_keys = now_included & previously_excluded
                self.quietly_included.update(new_keys)

            # Note that we don't worry about other cases (EXCLUDE + EXCLUDE, INCLUDE + EXCLUDE),
            # because quiet mode only handles fields that appear during merge, not those that disappear.

        # Done
        return self

    def get_full_projection(self):
        """ Generate a full, normalized projection for a model.

        This projection will contain all properties of a model, with 1-s and 0-s given for every
        field. It will take everything known to the class into account.

        This method always returns a copy.

        :rtype: dict
        """
        # In mixed mode, all columns are already there. Just return it
        if self.mode == self.MODE_MIXED:
            full_projection = self._projection.copy()
        else:
            # Generate a default full projection for every column
            # Meaning: {all: 0} or {all: 1}, depending on the mode
            full_projection = {name: 0 if self.mode == self.MODE_INCLUDE else 1
                               for name in self.supported_bags.names}

            # Overwrite it with the projection from the query
            full_projection.update(self._projection)

        # Force {key: 0} on every quietly_included one
        full_projection.update({key: 0 for key in self.quietly_included})

        # Done
        return full_projection

    def __contains__(self, name):
        """ Test whether a column name is included into projection (by name)

        :type name: str
        """
        if self.mode == self.MODE_MIXED:
            return name in self._projection and self._projection[name] == 1
        if self.mode == self.MODE_INCLUDE:
            return name in self._projection
        else:
            return name not in self._projection

    def pluck_instance(self, instance):
        """ Pluck an sqlalchemy instance and make it into a dict

            This method should be used to prepare an object for JSON encoding.
            It uses the information from the projection and "plucks" projected fields
            from an sqlalchemy object, putting them into a dict.

            This makes sure that only the properties explicitly requested by the user get included
            into the result, and *not* the properties that your code may have loaded.

            Note that this method knows nothing about relationships: you will have to add them
            yourself using the MongoJoin.pluck_instance() method and joining the two dictionaries.
            MongoQuery.pluck_instance() will do it for you.

            :param instance: object
            :rtype: dict
        """
        return {key: getattr(instance, key)
                for key, include in self.get_full_projection().items()
                if include
                and key not in self.quietly_included}

