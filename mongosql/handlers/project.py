from __future__ import absolute_import

from sqlalchemy.orm.base import InspectionAttr

from .base import MongoQueryHandlerBase
from ..bag import CombinedBag
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError
from ..util import Marker


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

    # Allow the handling of relationships by MongoProject
    RELATIONSHIPS_HANDLING_ENABLED = True

    def __init__(self, model, bags,
                 default_projection=None,
                 default_exclude=None,
                 default_exclude_properties=True,
                 default_unexclude_properties=None,
                 force_include=None, force_exclude=None,
                 raiseload_col=False):
        """ Init projection

        :param model: Sqlalchemy model to work with
        :param bags: Model bags
        :param default_projection: The default projection to use in the absence of any value
        :param default_exclude: A list of column names that are excluded even in exclusion mode.
            You can only get these properties if you request them explicitly.
            This only affects projections in exclusion mode: when the user has specified
            something like {id: 0, text: 0}, and the query would include a lot of fields,
            but you still want some of them removed by default.
            Use this for properties that contain a lot of data, or require extra queries.
        :param default_exclude_properties: By default, exclude @property and @hybrid_property attributes.
            This is a handy shortcut. Use `force_include` to overrule, or `default_exclude` manually to fine-tune.
        :param default_unexclude_properties: Exclude all but the given @property and @hybrid_property.
        :param force_include: A list of column names to include into the output always
        :param force_exclude: A list of column names to exclude from the output always
        :param raiseload_col: Install a raiseload_col() option on all fields excluded by projection.
            This is a performance safeguard: when your custom code uses certain fields, but a
            projection has excluded them, the situation will result in a LOT of extra queries!
            Solution: `raiseload_col=True` will raise an exception every time a deferred loading occurs;
            Make sure you manually do `.options(undefer())` on all the columns you need.
        """
        super(MongoProject, self).__init__(model, bags)

        # Settings
        self.default_projection = {k: Default(v)
                                   for k, v in (default_projection or {}).items()}
        self.default_exclude = set(default_exclude) if default_exclude else None
        self.force_include = set(force_include) if force_include else None
        self.force_exclude = set(force_exclude) if force_exclude else None
        self.default_exclude_properties = None
        self.raiseload_col = raiseload_col

        if default_exclude_properties or default_unexclude_properties:  # when either is specified, the effect is the same
            assert not default_unexclude_properties or default_exclude_properties, \
                'Using `default_unexclude_properties` only makes sense with default_exclude_properties=True'

            self.default_exclude_properties = self.bags.properties.names | self.bags.hybrid_properties.names
            self.default_exclude_properties -= set(default_unexclude_properties or ())
            # Merge `properties` and `hybrid_properties` into `default_exclude`
            self.default_exclude = (self.default_exclude or set()) | self.default_exclude_properties

        # On input
        #: Projection mode: self.MODE_INCLUDE, self.MODE_EXCLUDE, self.MODE_MIXED
        self.mode = None
        #: Normalized projection: dict(key=0|1). Not a full projection: some keys may be missing
        self._projection = None
        #: The list of fields that are quietly included
        self.quietly_included = set()

        # Validate
        if self.default_projection:
            try:
                # just for the sake of validation: init MongoProject once
                self.__class__(self.model, self.bags).input({k: v
                                                             # Validate the whole thing, with the exception of relationships
                                                             for k, v in default_projection.items()
                                                             if k not in self.bags.relations})
            except InvalidColumnError as e:
                # Reraise with a custom error message
                raise InvalidColumnError(self.bags.model_name, e.column_name, 'project:default_projection')
        if self.default_exclude:
            self.validate_properties_or_relations(self.default_exclude, where='project:default_exclude')
        if self.force_include:
            self.validate_properties_or_relations(self.force_include, where='project:force_include')
        if self.force_exclude:
            self.validate_properties_or_relations(self.force_exclude, where='project:force_exclude')

    def __copy__(self):
        obj = super(MongoProject, self).__copy__()
        obj.quietly_included = obj.quietly_included.copy()
        return obj

    def validate_properties_or_relations(self, prop_names, where=None):
        prop_names = set(prop_names)

        # Remove relationships
        if self.RELATIONSHIPS_HANDLING_ENABLED:
            prop_names -= self.bags.relations.names

        # Validate the rest
        return super(MongoProject, self).validate_properties(prop_names, bag=None, where=where)

    def _get_supported_bags(self):
        return CombinedBag(
            col=self.bags.columns,
            hybrid=self.bags.hybrid_properties,
            prop=self.bags.properties,
            # NOTE: please do not add `self.bags.relations` here: relations are handled separately:
            # _input_process() plucks them out, and _pass_relations_to_mongojoin() forwards them to MongoJoin.
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
        self.mode, self._projection, relations = self._input_process(projection)

        # Settings: default_exclude, force_include, force_exclude
        if self.mode == self.MODE_EXCLUDE and self.default_exclude:
            # Add even more fields that are excluded by default
            # The only way to load them is to explicitly require them.
            # The value is marked with Default(0) so that merge() won't use it to overwrite anything
            self._projection.update({k: Default(0) for k in self.default_exclude})

        if self.force_include or self.force_exclude:
            self._input_process_force_include_exclude()

        # Relations
        self._pass_relations_to_mongojoin(relations)

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

        # Remove items that are relationships
        # This is only supported when there's a MongoQuery that binds the two together
        relations = {}
        if self.mongoquery and self.RELATIONSHIPS_HANDLING_ENABLED:
            for name in list(projection.keys()):
                # If the name happens to be a relationship
                if name in self.bags.relations:
                    # Remove it
                    value = projection.pop(name)

                    # When value=1, transform it into a proper MongoJoin value
                    if value:
                        relations[name] = {}
            # Every relationship that wasn't removed will cause validation errors

        # Validate keys
        self.validate_properties(projection.keys())

        # Validate values
        unique_values = set(projection.values())
        if unique_values == {0} or unique_values == set():  # an empty dict
            # all values are 0
            mode = self.MODE_EXCLUDE
        elif unique_values == {1}:
            # all values are 1
            mode = self.MODE_INCLUDE
        else:
            # The only case when we allow mixing 1-s and 0-s -- is a full projection
            # A full projection includes all fields.
            # A full projection includes all fields.
            full_projection_keys = set(projection.keys())
            full_projection_keys |= set(self.default_exclude or ())
            full_projection_keys |= set(self.force_include or ())
            full_projection_keys |= set(self.force_exclude or ())

            # Test if it's a full projection
            is_full_projection = set(projection.keys()) == self.supported_bags.names
            if is_full_projection:
                mode = self.MODE_MIXED
            else:
                raise InvalidQueryError('Dict projection values shall be all 0s or all 1s, '
                                        'or a full projection object with all fields')

        # Done
        return mode, projection, relations

    def _input_process_force_include_exclude(self):
        """ input(): process self.force_include and self.force_exclude """
        # force_include
        if self.force_include:
            self.merge(dict.fromkeys(self.force_include, 1))

        # force_exclude
        if self.force_exclude:
            self.merge(dict.fromkeys(self.force_exclude, 0))

    def _pass_relations_to_mongojoin(self, relations):
        """ When _input_process() detects relationships, it returns them as a separate dict.
        This method forwards them to MongoJoin handler.

        It also tests whether 'join' is enabled on the query.
        """
        # Give relations to MongoJoin
        if relations:
            # This code relies on MongoJoin having been input()ed
            assert self.mongoquery, 'MongoProject tried to pass a relationship to MongoJoin, ' \
                                    'but there is no MongoQuery to bind the two together'
            assert self.mongoquery.handler_join.input_received, \
                'MongoProject tried to pass a relationship to MongoJoin, '\
                'but MongoJoin has not yet been given a chance to process its input()'

            # Raise an error if the 'join' handler is disabled for this query
            self.mongoquery._raise_if_handler_is_not_enabled('join')

            # Pass it to MongoJoin
            self.mongoquery.handler_join.merge(relations)

    @staticmethod
    def _columns2names(columns):
        """ Convert a list[Column | str] to list[str] names """
        return [c.key
                if isinstance(c, InspectionAttr) else
                c
                for c in columns]

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
        # Short-circuit
        if self.mode == self.MODE_EXCLUDE:
            empty = ()

            # Short-circuit: empty projection
            # (by default, with input(None), mode=exclude, and project={}
            if not self._projection:
                return empty  # no restrictions: all fields are to be loaded

            # Short-circuit: projection not empty, but only contains @property and @hybrid_property attributes:
            # Those that are going to be ignored anyway
            if set(self._projection) == self.default_exclude_properties:
                return empty  # no restrictions: all fields are to be loaded

        # load_only() all those columns
        load_only_columns = self.compile_columns()
        ret = [as_relation.load_only(*load_only_columns)]

        # raiseload_col() on all the rest (if requested)
        if self.raiseload_col:
            ret.append(as_relation.raiseload_col('*'))

            # Undefer PKs (otherwise, raiseload_col() will get them)
            ret.append(as_relation.undefer(*(column for name, column in self.bags.pk)))

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
        if self.raiseload_col:
            # raiseload_rel
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
        mode, projection, relations = self._input_process(projection)

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
            # Defaults won't override anything, because the values are the same anyway.
            self._projection.update(projection)
        elif self.mode == self.MODE_MIXED:
            # merge(whatever) into self.mixed mode: just merge
            # mixed mode contains every column's info, so whatever projection comes in, they are compatible.
            self._projection.update(projection)
        elif mode == self.MODE_INCLUDE and self.mode == self.MODE_EXCLUDE:
            # merge(include) in self.exclude mode
            # These modes are incompatible. Got to use full projection
            self._projection = self.get_full_projection()
            self._projection.update({k: v
                                     for k, v in projection.items()
                                     # don't let defaults override solid values!
                                     # If the value that's going to override is a Default(),
                                     # and there used to be some value in the original projection,
                                     # leave the original value
                                     if not (isinstance(v, Default) and k in orig_projection)
                                     })
            self.mode = self.MODE_MIXED
        elif mode == self.MODE_EXCLUDE and self.mode == self.MODE_INCLUDE:
            # merge(exclude) in self.include mode: just drop banned keys
            # this is a short-cut
            drop_keys = set(projection.keys()) & set(self._projection.keys())
            for k in drop_keys:
                if not isinstance(projection[k], Default):  # don't let defaults destroy solid values!
                    self._projection.pop(k)
        else:
            raise RuntimeError('Unknown combination of self.mode and mode')

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
            elif mode == self.MODE_INCLUDE and orig_mode == self.MODE_MIXED:
                # originally MIXED, merged some more.
                # Possibly, with includes. Compare the two sets of 1-s.
                now_included = set(k for k, v in self._projection.items() if v == 1)
                previously_excluded = set(k for k, v in orig_projection.items() if v == 1)
                new_keys = now_included - previously_excluded
                self.quietly_included.update(new_keys)

            # Note that we don't worry about other cases (EXCLUDE + EXCLUDE, INCLUDE + EXCLUDE),
            # because quiet mode only handles fields that appear during merge, not those that disappear.

        # Relations
        self._pass_relations_to_mongojoin(relations)

        # Done
        return self

    def include_columns(self, columns):
        """ Include more columns into the projection

            Note: you can use column names, or the actual column attributes!
            Make sure you don't use python @property: they don't have a name :(

            :param columns: List of columns, or column names
            :type columns: list[str, sqlalchemy.orm.Column]
        """
        column_names = self._columns2names(columns)
        return self.merge(dict.fromkeys(column_names, 1))

    def exclude_columns(self, columns):
        """ Exclude more columns from the projection

            Note: you can use column names, or the actual column attributes!
            Make sure you don't use python @property: they don't have a name :(

            :param columns: List of columns, or column names
            :type columns: list[str, sqlalchemy.orm.Column]
        """
        column_names = self._columns2names(columns)
        return self.merge(dict.fromkeys(column_names, 0))

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


class Default(Marker):
    """ A wrapper for dictionary keys which marks a value that was put there by default.

        For instance, when `default_exclude` puts a key into the dictionary,
        the value is wrapped with Default(). This way, whoever uses our projection,
        can see that this specific value is a default value, not something inserted by the user.

        By using a Marker, we also allow overrides: whenever anyone inserts another value into the dictionary,
        the value wrapped with Default() gets replaced with a value that's not wrapped.

        Why is this important?
        We need it for the merge() method, which is sometimes called from MongoJoin.

        Consider the following situation:
            1. A relation has default_exclude=('column',)
            2. Then you join to this relationship, specify a projection: project=('column',), because you want it.
                Alright, you've overriden the default
            3. Another piece of code does this: ensure_loaded(relation-name)
            4. This triggers the creation of another implicit projection. An empty one.
                It will use `default_exclude` by default, and contain {'column': 0}
            5. This new projection is merge()ed into the original projection,
                and your {'column': 1} gets replaced with a default coming from elsewhere.

        By using markers on the values, we can enforce the following rule:
        real values will have priority over Default() values,
        and a merge() will never overwrite any existing value with a Default value.

        Alternatives.
        1. Teach MongoProject to keep track of default values
        2. implement a dict() which keeps track of default values in a set.
        Both of them seemed ugly. Therefore, markers.
    """
