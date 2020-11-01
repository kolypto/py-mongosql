"""
### Project Operation

Projection corresponds to the `SELECT` part of an SQL query.

In MongoDB terminology, *projection* is the process of selection a subset of fields from a document.

Your models have many fields, but you do not always need them all. Oftentimes, all you need is just a small number
of them. That's when you use this operation that *projects* some fields for you.

The `projéct` operation lets you list the fields that you want to have in the data you get from the API endpoint.
You do this by either listing the fields that you need (called *include mode*), or listing the fields that you
*do not* need (called *exclude mode*).

The resulting data query on the back-end will only fetch the fields that you've requested, potentially saving a lot
of bandwidth.

An example of a projection would look like this:

```javascript
$.get('/api/user?query=' + JSON.stringify({
    // only include the following fields
    project: ['id', 'first_name', 'last_name'],
}))
```

#### Syntax

The Project operation supports the following syntaxes:

* Array syntax.

    Provide an array of field names to be included.
    All the rest will be excluded.

    Example:

    ```javascript
    { project: ['login', 'first_name'] }
    ```

* String syntax

    Give a list of field names, separated by whitespace.

    Example:

    ```javascript
    { project: 'login first_name' }
    ```

* Object syntax.

    Provide an object of field names mapped to either a `1` (include) or a `0` (exclude).

    Examples:

    ```javascript
    { project: { 'a': 1, 'b': 1 } } # Include specific fields. All other fields are excluded
    { project: { 'a': 0, 'b': 0 } }  # Exclude specific fields. All other fields are included
    ```

    Note that you can't intermix the two: you either use all `1`s to specify the fields you want included,
    or use all `0`s to specify the fields you want excluded.

    NOTE: One special case is a so-called *full projection*: when your projection object mentions every single property
    of a model, then you're allowed to set `1`s to some, and `0`s to others in the same object. Use wisely.

#### Fields Excluded by Default
Note that some fields that exist on the model may not be included *by default*: this is something that
back-end developers may have configured with `default_exclude` setting on the server.

You will not receive those fields unless you explicitly require them.
This may be appropriate for some field that contain a lot of data, or require some calculation.

To include those fields, you have to request them explicitly: just use their name
in the list of fields that you request.

#### Related Models
Normally, in order to load a related model (say, user's `user_profile`, or some other data related to this model),
you would use the [Join Operation](#join-operation).

However, for convenience, you can now also load related models by just giving their name in the projection,
as if it was a field. For example:

```javascript
{ project: {
    id: 1,
    name: 1,
    user_articles: 1  // the related model will be loaded
}}
```

This request will load the related `user_articles` for you.

Note that some relationships will be disabled for security reasons.
"""

from sqlalchemy.orm.base import InspectionAttr

from .base import MongoQueryHandlerBase
from ..bag import CombinedBag, FakeBag, AssociationProxiesBag
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

        Supported columns:

        * Columns
        * Hybrid properties
        * Python Propeties (`@property`)
        * Association Proxies
        * Relationships
    """

    query_object_section_name = 'project'

    # Allow the handling of relationships by MongoProject
    RELATIONSHIPS_HANDLING_ENABLED = True

    def __init__(self, model, bags,
                 default_projection=None,
                 bundled_project=None,
                 default_exclude=None,
                 default_exclude_properties=True,
                 default_unexclude_properties=None,
                 force_include=None, force_exclude=None,
                 ensure_loaded=None,
                 raiseload_col=False,
                 legacy_fields=None):
        """ Init projection

        :param model: Sqlalchemy model to work with
        :param bags: Model bags
        :param default_projection: The default projection to use in the absence of any value.
            Note: a `None` will default to "include all fields"; an empty value (empty list, set, dict) will default
            to "exclude all fields".
        :param bundled_project: A dict of column names mapped to a list of column names.
            If the key is included, the values are included as well. Quietly.
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
        :param ensure_loaded: The list of columns to load at all times, but quietly (without adding them into the projection)
        :param raiseload_col: Install a raiseload_col() option on all fields excluded by projection.
            This is a performance safeguard: when your custom code uses certain fields, but a
            projection has excluded them, the situation will result in a LOT of extra queries!
            Solution: `raiseload_col=True` will raise an exception every time a deferred loading occurs;
            Make sure you manually do `.options(undefer())` on all the columns you need.
        """
        # Legacy
        self.legacy_fields = frozenset(legacy_fields or ())
        self.legacy_fields_not_faked = self.legacy_fields - bags.all_names  # legacy_fields not faked as a @property

        # Parent
        super(MongoProject, self).__init__(model, bags)

        # Settings
        if default_projection is None:
            self.default_projection = None
        else:
            if not isinstance(default_projection, dict):
                default_projection = dict.fromkeys(default_projection, 1)
            self.default_projection = {k: Default(v) for k, v in default_projection.items()}
        self.bundled_project = bundled_project or {}
        self.default_exclude = set(default_exclude) if default_exclude else None
        self.force_include = set(force_include) if force_include else None
        self.force_exclude = set(force_exclude) if force_exclude else None
        self.default_exclude_properties = None
        self.ensure_loaded = set(ensure_loaded) if ensure_loaded else None
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
        if self.bundled_project:
            # NOTE: bundled_project does not support relationships as keys yet
            self.validate_properties_or_relations(self.bundled_project, where='project:bundled_project')
            for association_proxy_name, names in self.bundled_project.items():
                self.validate_properties_or_relations(names, where='project:bundled_project[{}]'.format(association_proxy_name))
        if self.default_exclude:
            self.validate_properties_or_relations(self.default_exclude, where='project:default_exclude')
        if self.force_include:
            self.validate_properties_or_relations(self.force_include, where='project:force_include')
        if self.force_exclude:
            self.validate_properties_or_relations(self.force_exclude, where='project:force_exclude')
        if self.ensure_loaded:
            self.validate_properties_or_relations(self.ensure_loaded, where='project:ensure_loaded')

    def __copy__(self):
        obj = super(MongoProject, self).__copy__()
        obj._projection = obj._projection.copy() if obj._projection is not None else None
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
            assocproxy=self.bags.association_proxies,
            # NOTE: please do not add `self.bags.relations` here: relations are handled separately:
            # _input_process() plucks them out, and _pass_relations_to_mongojoin() forwards them to MongoJoin.
            legacy=FakeBag({n: None for n in self.legacy_fields}),
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

        # Settings: default_exclude
        if self.mode == self.MODE_EXCLUDE and self.default_exclude:
            # Add even more fields that are excluded by default
            # The only way to load them is to explicitly require them.
            # The value is marked with Default(0) so that merge() won't use it to overwrite anything
            self._projection.update({k: Default(0) for k in self.default_exclude})

        # bundled_project, force_include, force_exclude
        more_relations = self._settings_process_force_include_exclude_and_bundled_project()
        relations.update(more_relations)

        # Relations
        self._pass_relations_to_mongojoin(relations, strict=False)

        # ensure_loaded
        if self.ensure_loaded:
            self.merge(list(self.ensure_loaded), quietly=True, strict=False)

        # Done
        return self

    def _input_process(self, projection):
        """ input(): receive, validate, preprocess """
        # Empty projection
        # This logic differentiates between `None` as input, meaning, no value was provided;
        # and an empty list|dict, which means that the user explicitly stated that they do not want any fields.
        if projection is None:
            # No projection provided
            # See how the default value applies
            if self.default_projection is None:
                # No default value given in the settings
                # MongoSQL defaults to the inclusion of all fields
                projection = {}
                default_mode = self.MODE_EXCLUDE
            elif not self.default_projection:
                projection = {}
                default_mode = self.MODE_INCLUDE
            else:
                projection = self.default_projection
                default_mode = self.MODE_INCLUDE if set(self.default_projection.values()) == {1} else self.MODE_EXCLUDE
        elif not projection:
            # Empty projection: the user does not want any fields
            # This means empty include list: include nothing
            projection = {}
            default_mode = self.MODE_INCLUDE
        else:
            # A projection provided to input().
            # The default mode depends on the actual values.
            default_mode = None

        # String syntax
        if isinstance(projection, str):
            # Split by whitespace and convert to dict
            projection = dict.fromkeys(projection.split(), 1)

        # Array syntax
        if isinstance(projection, (list, tuple)):
            # Convert to dict
            projection = dict.fromkeys(projection, 1)

        # Dict syntax
        if not isinstance(projection, dict):
            raise InvalidQueryError('Projection must be one of: null, string, array, object; '
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

                    # Handle this ugly case when a projection only had a few relationships and no columns.
                    # In this case, the `projection` dict became empty, and it did not remember the mode it was in.
                    if default_mode is None:
                        default_mode = 1 if value else 0

                    # When value=1, transform it into a proper MongoJoin value
                    if value == 1:
                        value = {}

                    # Save it
                    relations[name] = value
            # Every relationship that wasn't removed will cause validation errors

        # Validate keys
        self.validate_properties(projection.keys())

        # Validate values
        unique_values = set(projection.values())
        if not projection:
            # Empty projection
            mode = default_mode
        elif unique_values == {0}:
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

    def _process_simple_merge(self, mode, projection, merge_projection, quietly_included=()):
        """ Simply merge two projections: merge `merge_projection` into (mode, projection) and return it """
        # Prepare the input
        merge_mode, merge_projection, merge_relations = self._input_process(merge_projection)

        # Merge (merge_mode, merge_projection) into (mode, projection)

        # Now, the logic goes as follows.
        # When the two modes are compatible (mode == merge_mode), we can just update() the dict.
        # But when the two modes are incompatible (e.g., one in inclusion mode, and one in exclusion mode),
        # We have to use the full projection object, and update it.
        if mode == merge_mode:
            # Compatible modes: just merge
            # Defaults won't override anything, because the values are the same anyway.
            projection.update(merge_projection)
        elif mode == self.MODE_MIXED:
            # merge(whatever) into a MIXED mode: just merge
            # mixed mode contains every column's info, so whatever projection comes in, they are compatible.
            projection.update(merge_projection)
        elif merge_mode == self.MODE_MIXED:
            # merge(MIXED) into whatever: complete override
            projection = merge_projection
            mode = self.MODE_MIXED
        elif merge_mode == self.MODE_INCLUDE and mode == self.MODE_EXCLUDE:
            # merge(include) into an EXCLUDE mode
            # These modes are incompatible. Got to use full projection
            orig_projection = projection.copy()
            projection = self._generate_full_projection_for(mode, projection, quietly_included=quietly_included)
            projection.update({k: v
                               for k, v in merge_projection.items()
                               # don't let defaults override solid values!
                               # If the value that's going to override is a Default(),
                               # and there used to be some value in the original projection,
                               # leave the original value
                               if not (isinstance(v, Default) and k in orig_projection)
                               })
            mode = self.MODE_MIXED
        elif merge_mode == self.MODE_EXCLUDE and mode == self.MODE_INCLUDE:
            # merge(exclude) in self.include mode: just drop banned keys
            # this is a short-cut
            drop_keys = set(merge_projection.keys()) & set(projection.keys())
            for k in drop_keys:
                if not isinstance(merge_projection[k], Default):  # don't let defaults destroy solid values!
                    projection.pop(k)
        else:
            raise RuntimeError('Unknown combination of merge_mode and mode')

        return mode, projection, merge_relations

    def _settings_process_force_include_exclude_and_bundled_project(self):
        """ Process force_include, force_exclude, bundled_project """
        relations = {}

        # force_include
        if self.force_include:
            self.mode, self._projection, more_rels = \
                self._process_simple_merge(self.mode, self._projection, dict.fromkeys(self.force_include, 1))
            relations.update(more_rels)

        # force_exclude
        if self.force_exclude:
            self.mode, self._projection, more_rels = \
                self._process_simple_merge(self.mode, self._projection, dict.fromkeys(self.force_exclude, 0))
            relations.update(more_rels)

        # bundled_project
        # Got to do it last, because you never know who might've added more keys
        if self.bundled_project:
            more_keys = set()
            for bundle_key, bundled_keys in self.bundled_project.items():
                if bundle_key in self:
                    # Only add those that are not already added.
                    # Otherwise, we may end up "quieting up" keys that were explicitly requested
                    more_keys.update({key
                                      for key in bundled_keys
                                      if key not in self})

            # Merge
            self.mode, self._projection, more_rels = \
                self._process_simple_merge(self.mode, self._projection, dict.fromkeys(more_keys, 1))
            relations.update(more_rels)

            # Those bundled columns must be included quietly
            self.quietly_included.update(more_keys)
            # TODO: relationships should also be included quietly

        # Done
        return relations

    def _input_process_bundled_project(self):
        """ input(): process self.bundled_project """
        for bundle_key, bundled_keys in self.bundled_project.items():
            # A bundle key is included
            if bundle_key in self:
                # See if there're any keys we have to include
                missing_bundle_keys = {k
                                       for k in bundled_keys
                                       if k not in self}
                # Merge
                self.merge(dict.fromkeys(missing_bundle_keys, 1))

    def _generate_full_projection_for(self, mode, projection, quietly_included=()):
        """ Generate a copy of a full projection for the given (mode, projection) """
        # In mixed mode, all columns are already there. Just return it
        if mode == self.MODE_MIXED:
            full_projection = projection.copy()
        else:
            # Generate a default full projection for every column
            # Meaning: {all: 0} or {all: 1}, depending on the mode
            full_projection = {name: 0 if mode == self.MODE_INCLUDE else 1
                               for name in self.supported_bags.names}

            # Overwrite it with the projection from the query
            full_projection.update(projection)

        # Force {key: 0} on every quietly_included one
        full_projection.update({key: 0 for key in quietly_included})

        # Done
        return full_projection

    def _pass_relations_to_mongojoin(self, relations, strict):
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
            self.mongoquery.handler_join.merge(relations, strict=strict)

    @staticmethod
    def _columns2names(columns):
        """ Convert a list[Column | str] to list[str] names """
        return [c.key
                if isinstance(c, InspectionAttr) else
                c
                for c in columns]

    def _compile_list_of_included_columns_from_bag(self, bag):
        """ Generate a list of columns, using a bag as a reference point.

        Will generate a list of attributes included from that particular bag by the current projection.

        When `self.bags.columns` is used, it will generate a list of included columns only.
        When `self.bags.association_proxies` is used, lists only included Association Proxy proxies.
        """
        if self.mode == self.MODE_INCLUDE or self.mode == self.MODE_MIXED:
            # Only {col: 1}
            return [bag[col_name]
                    for col_name, include in self._projection.items()
                    if include == 1 and col_name in bag]
        else:
            # Exclude mode
            # All, except {col: 0}
            return [column
                    for col_name, column in bag
                    if col_name not in self._projection]

    def compile_columns(self):
        """ Get the list of columns to be included into the Query """
        # Note that here we do not iterate over self.supported_bags
        # Instead, we iterate over self.bags.columns, because properties and hybrid properties do
        # not need to be loaded at all!
        return self._compile_list_of_included_columns_from_bag(self.bags.columns)

    def compile_options(self, as_relation):
        """ Get the list of options for a Query: load_only() for columns, and some eager loaders for relationships """
        options = []
        options.extend(self._compile_column_options(as_relation))
        options.extend(self._compile_relationship_options(as_relation))
        return options

    def _compile_column_options(self, as_relation):
        """ Column options: Get the list of load_only() options for a Query """
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

        # NOTE: we don't have to ignore legacy_fields here because compile_columns() only goes through real columns

        # load_only() all those columns
        load_only_columns = self.compile_columns()
        options = [as_relation.load_only(*load_only_columns)]

        # raiseload on all other columns
        options.extend(self._compile_raiseload_options(as_relation))

        # Done
        return options

    def _compile_raiseload_options(self, as_relation):
        """ Column options: raiseload_col() on all other columns """
        if self.raiseload_col:
            return [
                # raiseload_col() on all the rest
                as_relation.raiseload_col('*'),
                # Undefer PKs (otherwise, raiseload_col() will get them)
                as_relation.undefer(*(column for name, column in self.bags.pk)),
            ]

        # done
        return ()

    def _compile_relationship_options(self, as_relation):
        """ Relationship options: for relationships that are affected by this projection.

        Currently, only used by Association Proxies: when you include one of them, MongoProject has to load that
        relationship in order to get the property values
        """
        # Get the list of included association proxies
        assproxx = self._compile_list_of_included_columns_from_bag(self.bags.association_proxies)

        # Convert that to the list of underlying relationships, and load it's most important property
        return [
            # selectinload() + load_only()
            as_relation.selectinload(
                # Get the underlying relationship, properly aliased
                self.bags.association_proxies.get_relationship(association_proxy)
            ).load_only(association_proxy.remote_attr.key)  # TODO: does not work with aliased relationships
            for association_proxy in assproxx
        ]

    # Not Implemented for this Query Object handler
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation):
        assert as_relation is not None
        return query.options(self.compile_options(as_relation))

    # Extra features

    def merge(self, projection, quietly=False, strict=False):
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
        :param strict: Whether to do a strict merge (see MongoJoin, which will refuse to merge()
            in case of incompatible filters)
        :type strict: bool
        :type quietly: bool
        :rtype: MongoProject
        """
        # Make a copy because we're going to modify it
        orig_mode = self.mode
        orig_projection = self._projection

        # Merge
        new_mode, new_projection, relations = \
            self._process_simple_merge(orig_mode, orig_projection.copy(), projection, self.quietly_included)

        # Apply
        self.mode = new_mode
        self._projection = new_projection

        # Quiet mode handler
        if quietly:
            # Only handle cases where more keys were included
            if new_mode == self.MODE_INCLUDE and orig_mode == self.MODE_INCLUDE:
                # originally INCLUDE, merge INCLUDE
                # More keys included
                new_keys = set(new_projection.keys()) - set(orig_projection.keys())
                self.quietly_included.update(new_keys)
            elif orig_mode == self.MODE_EXCLUDE and new_mode == self.MODE_MIXED:
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
            elif orig_mode == self.MODE_MIXED:
                # originally MIXED, merged some more.
                # Possibly, with includes. Compare the two sets of 1-s.
                now_included = set(k for k, v in self._projection.items() if v == 1)
                previously_excluded = set(k for k, v in orig_projection.items() if v == 1)
                new_keys = now_included - previously_excluded
                self.quietly_included.update(new_keys)

            # Note that we don't worry about other cases (EXCLUDE + EXCLUDE, INCLUDE + EXCLUDE),
            # because quiet mode only handles fields that appear during merge, not those that disappear.


        # Relations
        self._pass_relations_to_mongojoin(relations, strict=strict)

        # bundled_project, force_include, force_exclude
        more_relations = self._settings_process_force_include_exclude_and_bundled_project()
        # Any relations that have come up at this point must be bundled_project properties, nothing more.
        # Therefore, it must be safe to load them in non-strict mode.
        self._pass_relations_to_mongojoin(more_relations, strict=False)

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

    @property
    def projection(self):
        """ Get the current projection as a dict

            Depending on self.mode, it can be:

            self.mode = MODE_INCLUDE:   all {key: 1}
            self.mode = MODE_EXCLUDE:   all {key: 0}
            self.mode = MODE_MIXED:     mixed {key: 0, key: 1}, but having every key of the model
        """
        proj = self._projection.copy()

        # Force 0s on quietly included fields
        if self.quietly_included:
            # Do a proper merge
            _, proj, _ = self._process_simple_merge(self.mode, proj, dict.fromkeys(self.quietly_included, 0))

        return proj

    def get_full_projection(self):
        """ Generate a full, normalized projection for a model.

        This projection will contain all properties of a model, with 1-s and 0-s given for every
        field. It will take everything known to the class into account.

        This method always returns a copy.

        :rtype: dict
        """
        return self._generate_full_projection_for(self.mode, self._projection, self.quietly_included)

    def get_final_input_value(self):
        # Make sure that Default() does not make it out. Otherwise, jsonify() would fail on it
        return {k: Default.unwrap(v)
                for k, v in self.projection.items()}

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
                and key not in self.quietly_included
                and key not in self.legacy_fields_not_faked}


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
