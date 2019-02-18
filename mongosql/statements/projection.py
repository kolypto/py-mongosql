from __future__ import absolute_import

from .base import _MongoQueryStatementBase
from ..bag import CombinedBag
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


# TODO: implement a Projection capable of JOINing (that also handles relationships and nested
#  properties; support dot-notation). This should be a different class, because then you can just
#  give it a different projection handler
class MongoProjection(_MongoQueryStatementBase):
    """ MongoDB projection operator.

        This operator is essentially the one that enables you to choose which fields to select
        from a query.

        Syntax in Python:

        * None: use default (include all)
        * { a: 1, b: 1 } - include only the given fields; exclude all the rest
        * { a: 0, b: 0 } - exclude the given fields; include all the rest
        * [ a, b, c ] - include only the given fields

        Supports: Columns, Properties, Hybrid Properties

        Note: if you want a projection that ONLY supports columns and does not handle properties
        at all, you've got two options:
        1. Specify model properties as `force_exclude`, and they will always be excluded
        2. Subclass, and override _get_supported_bags() with `return self.bags.columns`.
            Then, input() will validate the input against columns only, disallowing properties

        Other useful methods:
        * get_full_projection() will compile a full projection: a projection that contains every
            column of a model, mapped to 1 or 0, depending on whether the user wanted it.
        * __contains__() will test whether a column was requested by this projection operator:
            p = MongoProjection(Article).input(...)
            if 'title' in p: ...
    """

    query_object_section_name = 'project'

    def __init__(self, model, default_projection=None,
                 default_exclude=None,
                 force_include=None, force_exclude=None):
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
        """
        super(MongoProjection, self).__init__(model)

        self.default_projection = default_projection or None
        self.default_exclude = set(default_exclude) if default_exclude else None
        self.force_include = set(force_include) if force_include else None
        self.force_exclude = set(force_exclude) if force_exclude else None

        # On input
        #: Projection mode: self.MODE_INCLUDE, self.MODE_EXCLUDE, self.MODE_MIXED
        self.mode = None
        #: Normalized projection: dict(key=0|1). Not a full projection: some keys may be missing
        self.projection = None

        # Validate
        if self.default_projection:
            # just for the sake of validation
            MongoProjection(self.model).input(default_projection)
        if self.default_exclude:
            self.validate_properties(self.default_exclude, where='projection:default_exclude')
        if self.force_include:
            self.validate_properties(self.force_include, where='projection:force_include')
        if self.force_exclude:
            self.validate_properties(self.force_exclude, where='projection:force_exclude')

    def _get_supported_bags(self):
        return CombinedBag(
            col=self.bags.columns,
            hybrid=self.bags.hybrid_properties,
            prop=self.bags.properties,
        )

    #: MongoSQL projection statement operation modes
    #: Projection statement can operate in three modes
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
        super(MongoProjection, self).input(projection)

        # Prepare
        self.projection = projection
        self.mode = None  # undecided

        # Process
        self.mode, self.projection = self._input_validate(projection)
        if self.force_include or self.force_exclude:
            self._input_process_force_include_exclude()

        # Done
        assert self.mode is not None  # somebody should have decided by now
        return self

    def _input_validate(self, projection):
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
            raise InvalidQueryError('Projection must be one of: None, list, object; '
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

        # default_exclude
        if mode == self.MODE_EXCLUDE and self.default_exclude:
            # Add even more fields that are excluded by default
            # The only way to load them is to explicitly require them.
            projection.update({k: 0 for k in self.default_exclude})

        return mode, projection

    def _input_process_force_include_exclude(self):
        """ input(): process self.force_include and self.force_exclude """
        # Make a copy because we're going to modify it
        self.projection = self.projection.copy()

        # force_include
        if self.force_include:
            if self.mode == self.MODE_INCLUDE:
                # force_include in include mode: just add more keys
                self.projection.update({k: 1 for k in self.force_include})
            else:
                # force_include in exclude mode: these modes are incompatible
                # Got to use full projection, and specify 1-s and 0-s individually
                self.projection = self.get_full_projection()
                self.mode = self.MODE_MIXED
        # force_exclude
        if self.force_exclude:
            if self.mode == self.MODE_EXCLUDE:
                # force_exclude in exclude mode: just add more keys
                self.projection.update({k: 0 for k in self.force_exclude})
            else:
                # force_exclude in include mode: drop banned keys
                drop_keys = set(self.force_exclude) & set(self.projection.keys())
                for k in drop_keys:
                    self.projection.pop(k)

    def compile_columns(self):
        """ Get the list of columns to be included into the Query """
        # Note that here we do not iterate over self.supported_bags
        # Instead, we iterate over self.bags.columns, because properties and hybrid properties do
        # not need to be loaded at all!
        if self.mode == self.MODE_MIXED or self.mode == self.MODE_INCLUDE:
            # Only {col: 1}
            return [self.bags.columns[col_name]
                        for col_name, include in self.projection.items()
                        if include == 1 and col_name in self.bags.columns]
        else:
            # Exlude mode
            # All, except {col: 0]
            return [column
                    for col_name, column in self.bags.columns
                    if col_name not in self.projection]

    def compile_options(self, as_relation):
        """ Get the list of load_only() options for a Query """
        # TODO: is raiseload() support for projections? Let the user specify that they do not
        #  want excluded properties at all; moreover, an exception should be raised
        return [as_relation.load_only(c)
                for c in self.compile_columns()]

    # Extra features

    def get_full_projection(self):
        """ Generate a full, normalized projection for a model.

        This projection will contain all properties of a model, with 1-s and 0-s given for every
        field. It will take everything known to the class into account.

        This method always returns a copy.

        :rtype: dict
        """
        # In mixed mode, all columns are already there. Just return it
        if self.mode == self.MODE_MIXED:
            return self.projection.copy()
        # Generate a default full projection for every column
        full_projection = {name: 0 if self.mode == self.MODE_INCLUDE else 1
                           for name in self.supported_bags.names}
        # Overwrite it with the projection from the query
        full_projection.update(self.projection)
        # Done
        return full_projection

    def __contains__(self, item):
        """ Test whether a column is included into projection (by name)

        :type item: str
        """
        if self.mode == self.MODE_MIXED:
            return item in self.projection and self.projection[item] == 1
        if self.mode == self.MODE_INCLUDE:
            return item in self.projection
        else:
            return item not in self.projection
