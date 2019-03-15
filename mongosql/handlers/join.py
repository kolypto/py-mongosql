from __future__ import absolute_import
from sqlalchemy.orm import aliased, Query

from .base import MongoQueryHandlerBase
from ..exc import InvalidQueryError, DisabledError, InvalidColumnError, InvalidRelationError


class MongoJoin(MongoQueryHandlerBase):
    """ MongoSql handler for eagerly loading related models.

        Supports the following arguments:

        - List of relation names
        - Dict: { relation-name: query-dict } for MongoQuery.query
    """

    query_object_section_name = 'join'

    def __init__(self, model, allowed_relations=None, banned_relations=None, raiseload=False):
        """ Init a join expression

        :param allowed_relations: List of relations that can be joined
        :param banned_relations: List of relations that can't be joined to
        :param raiseload: Install a raiseload() option on all relations not explicitly loaded.
            This is a performance safeguard for the cases when your code might use them.
        """
        super(MongoJoin, self).__init__(model)

        # Security
        if allowed_relations is not None and banned_relations is not None:
            raise AssertionError('Cannot use both `allowed_relations` and `banned_relations`')
        elif allowed_relations:
            self.allowed_relations = set(allowed_relations)
        elif banned_relations:
            self.allowed_relations = self.bags.relations.names - set(banned_relations)
        else:
            self.allowed_relations = None

        # Raiseload?
        self.raiseload = raiseload

        # Validate
        if self.allowed_relations:
            self.validate_properties(self.allowed_relations, where='join:allowed_relations')

        # On input
        # type: list[MongoJoinParams]
        self.mjps = None

    def _get_supported_bags(self):
        return self.bags.relations

    def _get_relation_insecurely(self, relation_name):
        """ Get a relationship. Insecurely. Disrespect `self.allowed_relations`. """
        try:
            return self.bags.relations[relation_name]
        except KeyError:
            raise InvalidRelationError(self.bags.model, relation_name, 'join')

    def _get_relation_securely(self, relation_name):
        """ Get a relationship. Securely. Respect `self.allowed_relations`. """
        # Get it
        relation = self._get_relation_insecurely(relation_name)
        # Check it
        if self.allowed_relations is not None:
            if relation_name not in self.allowed_relations:
                raise DisabledError('Join: joining is disabled for relationship `{}`'
                                    .format(relation_name))
        # Yield it
        return relation

    def validate_properties(self, prop_names, bag=None, where=None):
        # Use the same logic, but remake the error into InvalidRelationError
        try:
            return super(MongoJoin, self).validate_properties(prop_names, bag, where)
        except InvalidColumnError as e:
            raise InvalidRelationError(e.model, e.column_name, e.where)

    def input(self, rels):
        super(MongoJoin, self).input(rels)

        # Validation
        if not rels:
            rels = {}
        elif isinstance(rels, (list, tuple)):
            rels = {relname: None for relname in rels}
        elif isinstance(rels, dict):
            rels = rels
        else:
            raise InvalidQueryError('Join must be one of: null, array, object')

        self.validate_properties(rels.keys())
        self.rels = rels

        # Go over all relationships and simply build MJP objects that will carry the necessary
        # information to the Query on the outside, which will use those MJP objects to handle the
        # actual joining process
        mjp_list = []
        for relation_name, query_object in self.rels.items():
            # Get the relationship and its target model
            rel = self._get_relation_securely(relation_name)
            target_model = self.bags.relations.get_target_model(relation_name)
            target_model_aliased = aliased(rel)  # aliased(rel) and aliased(target_model) is the same thing

            # Prepare the nested MongoQuery
            # We do it here so that all validation errors come on input()
            nested_mongoquery = self.mongoquery._get_nested_mongoquery(
                relation_name,
                target_model,
                target_model_aliased
            ).query(**query_object or {})

            # Start preparing the MJP: MongoJoinParams.
            mjp = MongoJoinParams(
                model=self.bags.model,
                bags=self.bags,
                relationship_name=relation_name,
                relationship=rel,
                target_model=target_model,
                # Got to use an alias because when there are two relationships to the same model,
                # it would fail because of ambiguity
                target_model_aliased=target_model_aliased,
                query_object=query_object,
                nested_mongoquery=nested_mongoquery
            )

            # Add the newly constructed MJP to the list
            mjp_list.append(mjp)

        self.mjps = mjp_list
        return self

    # Not Implemented for this Query Object handler
    compile_options = NotImplemented
    compile_columns = NotImplemented
    compile_statement = NotImplemented
    compile_statements = NotImplemented

    def alter_query(self, query, as_relation):
        assert as_relation is not None
        assert self.mongoquery is not None, 'MongoJoin can only work when bound with_mongoquery() to a MongoQuery'

        # Process joins
        for mjp in self.mjps:
            if not mjp.has_nested_query:
                query = self._load_relationship_no_filter(query, as_relation, mjp)
            else:
                query = self._load_relationship_with_filter(query, as_relation, mjp)

        # Put a raiseload() on every other relationship!
        if self.raiseload:
            as_relation.raiseload('*')

        return query

    # region Protected: Eager Loading Implementations

    def _load_relationship_no_filter(self, query, as_relation, mjp):
        """ Load a relationship when there's no filtering query present

            This method just uses SqlAlchemy's eager loading.
        """
        # There is no nested Query: all we have to do is just to load the relationship.
        # In this case, it's sufficient to use sqlalchemy eager loading.

        # Alright, which strategy?
        # We will do it as follows:
        # If uselist=False, then joinedload()
        # If uselist=True, then selectinload()
        if mjp.uselist:
            rel_load = as_relation.selectinload(mjp.relationship)
        else:
            rel_load = as_relation.joinedload(mjp.relationship)
            # Make sure there's no column name clash in the results
            query = query.with_labels()

        # We still have to let the nested MongoQuery run its business
        # It may be installing projections even when there's no Query Object:
        # because there are default settings, and sometimes the user does not get what he wants, but what we want :)
        query = mjp.nested_mongoquery.from_query(query).end()

        # Since there's no Query Object, there's no projection nor join provided.
        # This means that the user does not want sub-relations, so we don't load them.
        if self.raiseload:
            rel_load.raiseload('*')
        else:
            rel_load.lazyload('*')  # deferred loading upon request

        # Done here
        return query.options(rel_load)

    def _load_relationship_with_filter(self, query, as_relation, mjp):
        """ Load a relationship when a filter is present """
        # A user has requested a relationship, and also provided a Query Object.
        # In this case, we will use another MongoQuery to build a query that loads them.

        # Previously, MongoSQL used join()s, but it resulted in a whole bunch of issues.
        # Even when you had used join(isouter=True), any filter applied to the whole query
        # resulted in the loss of your original entities! Because when there was no related
        # model, and the joined column was null ... the whole row was dropped.

        # There can be multiple solutions.
        # 1. Make the nested MongoQuery in a subquery.
        #    The subquery will generate & filter all the related entities, and then our
        #    primary query can just join to it.
        #    In this case, the nested condition will be isolated inside the subquery and
        #    will not distort the results of primary query.
        # 2. LEFT OUTER JOIN the relationship with ON-clause.
        #    Perform the regular .join(relationship), but put all filtering on that
        #    relationship into the ON-clause for the join.
        #    This will ensure that the results are not distorted, and the related entities
        #    are only joined to it when present.
        # 3. Use a join(), but put an additional condition on the whole query that a related
        #    column (e.g. primary key) may be NULL
        # 4. Load these related entities separately: the likes of selectinload()

        # We choose: #2: LEFT OUTER JOIN + ON clause

        if mjp.uselist:
            # Use left_outer_join for lists # TODO: use selectinload for lists!
            return self._load_relationship_with_filter__left_outer_join(query, as_relation, mjp)
        else:
            # Use left_outer_join for foreign-key relations
            return self._load_relationship_with_filter__left_outer_join(query, as_relation, mjp)

    def _load_relationship_with_filter__left_outer_join(self, query, as_relation, mjp):
        """ Load a relationship with LEFT OUTER JOIN and filter it.

            This will do a .join(isouter=True) to the related entity, producing a LEFT OUTER JOIN,
            and will put the filter condition into the ON clause instead of the WHERE clause.

            Example:

                Article.mongoquery(ssn).query({  # pseudo-JSON syntax for clarity
                    join:
                        user:
                            filter: age>18
                }).end()

                SELECT articles.*, users.*
                FROM articles
                    LEFT JOIN users ON users.id = articles.uid
                                    AND users.age > 18;

                This query will give you all articles,
                and only include authors when they're old enough.

            Loading a relationship with LEFT OUTER JOIN has its issues:

            * When a single entity in the original query has many related entities,
              it will produce a lot of duplicate data sent to the client.
              Example: N users, joined to K articles each, will yield N*K rows.
            * The number of rows in the resulting query will not be right either.
            * It does not permit grouping, skipping, and limiting:
              GROUP BY, SKIP, and LIMIT would modify the original query and distort its results.
            * We've had a LOT of headache with bulding this query.. :)

            :type Query: sqlalchemy.orm.Query
        """
        # Check the Query Object
        for unsupported in ('aggregate', 'group', 'skip', 'limit'):
            if unsupported in mjp.query_object:
                raise InvalidQueryError('MongoSQL does not support `{}` for joined queries'
                                        .format(unsupported))

        # If our source model is aliased, we have to use its alias in the query
        # self.model is that very thing: it's aliased, if we're aliased()
        source_model_aliased = self.model

        # There will be a few special cases with the ORDER BY clause, so let's get the handler
        project_handler = self.mongoquery.handler_project  # type: MongoProject
        sort_handler = self.mongoquery.handler_sort  # type: MongoSort

        # Handle the situation when the outer query (the top-level query) has a LIMIT
        # In this case, when we JOIN, there's going to be a problem: rows would multiply, and LIMIT won't do what
        # it is supposed to do.
        # To prevent this, we do the following trick: we take this query, with limits applied to it,
        # and make it into a subquery, like this:
        # SELECT users.*, articles.*
        # FROM (
        #   SELECT * FROM users WHERE ... LIMIT 10
        #   ) AS users
        #   LEFT JOIN articles ....
        if query._limit is not None or query._offset is not None:  # accessing protected properties of Query
            # We're going to make it into a subquery, so let's first make sure that we have enough columns selected.
            # We'll need columns used in the ORDER BY clause selected, so let's get them out, so that we can use them
            # in the ORDER BY clause later on (a couple of statements later)
            order_by_column_names = [c.key or c.element.key
                                     for c in sort_handler.compile_columns()]
            # If there even is any ordering?
            if order_by_column_names:
                # undefer() every column that participates in the ORDER BY
                # we'll have to undo it later on
                query = query.options(*[as_relation.undefer(column_name)
                                        for column_name in order_by_column_names])

            # Select from self, so that LIMIT stays inside the inner query
            query = query.from_self()

            # Handle the 'ORDER BY' clause of the main query.
            # We can't let it stay inside the subquery: otherwise, the main ordering won't be specified, and related
            # queries will define the ordering of the outside scope! That's unacceptable.
            #
            # Example: mongoquery(User) { sort: [age+], limit: 10, join: { articles: { sort=[rating-] } }
            # Currently, a query will loook like this:
            #   SELECT users.*, articles.*
            #   FROM (
            #       SELECT * FROM users
            #       ORDER BY users.age
            #       LIMIT 10
            #       ) AS users
            #       LEFT JOIN articles ...
            #   ORDER BY articles.rating DESC.
            #
            # It's clear that we have to take the 'ORDER BY' clause from the inside, and duplicate it on the outside.

            # Ordering will always be present inside the subquery, because the 'sort' handler gets executed before 'join'.
            # Now we have to add another ordering to the outside query.

            # Test if there even was any sorting?
            if not sort_handler.is_input_empty():
                # Apply ORDER BY again, but to the outside query
                query = sort_handler.alter_query(query)
                # Undo undefer()ed columns: restore the columns to its original loader option (as specified by projection)
                query = query.options(*[
                    project_handler.compile_option_for_column(column_name, as_relation)
                    for column_name in order_by_column_names
                ])

        # Initialize the nested MongoQuery
        # as_relation() and aliased() already configured for us
        nested_mq = mjp.nested_mongoquery

        # Build a LEFT OUTER JOIN from `query` to the `target_model`, through the `relationship`
        query = _left_outer_join_with_filter(
            # The query to build the join from
            query,
            # Source model can be aliased, because this join may be a 2nd or even 3rd level.
            # This alias is used to adapt the JOIN condition for a relationship
            # (that is, in the model you might specify "user.id = article.uid",
            #  and both references have to use proper aliases. This is called "adaptation")
            source_model_aliased,
            # The relationship to join through.
            # This relationship contains the default JOIN condition
            mjp.relationship,
            # The target model alias to use when adapting the statements
            mjp.target_model_aliased,
            # The additional filter clause to be added as the ON-clause for the JOIN.
            # This is where the magic happens.
            # Not that the nested MongoQuery is already using proper aliases for both
            # the source model and the target model, so the compiled statement will reference
            # them correctly.
            nested_mq.handler_filter.compile_statement()
        )

        # Because we've already used the filter statement into the ON clause,
        # we have to remove it from MongoQuery!
        nested_mq.handler_filter.expressions = []  # reset!

        # Now, nested MongoQuery may contain additional statements
        # Projection, sorting, etc.
        # It's time to add it to the query.
        query = nested_mq.from_query(query).end()

        # Now, when there are many different models joined in one query, we'll have name clashes.
        # To prevent that, with_labels() will give unique names to every column.
        query = query.with_labels()

        # Now, the query contains all the results.
        # Now we use `contains_eager()` to tell sqlalchemy that the resulting rows
        # will contain the relationship, so that it can pick them up.
        return query.options(
            as_relation.contains_eager(
                mjp.relationship,
                # It is important to tell sqlalchemy that the table is aliased.
                alias=mjp.target_model_aliased))

    def _load_relationship_with_filter__joinf(self, query, as_relation, mjp):
        """ Load a relationship with JOIN and filter it, putting the condition into WHERE.

            Note that this will distort the results of the original query:
            essentially, it will only return entities *having* at least one related entity with
            the given condition.

            Example:

                Article.mongoquery(ssn).query({  # pseudo-JSON syntax for clarity
                    join:
                        user:
                            filter: age>18
                }).end()

                SELECT articles.*, users.*
                FROM articles
                    JOIN users ON users.id = articles.uid
                WHERE users.age > 18;

                This query will give you all articles whose author is old enough.
                Articles from youngsters will not be included.

            Loading a relationship with JOIN has its issues:

            * Distorts the results of the original query (unless that's the intended behavior)
            * Loads a lot of duplicate rows
            * It does not permit grouping, skipping, and limiting:
              GROUP BY, SKIP, and LIMIT would modify the original query and distort its results.
            * Has wrong COUNT
        """
        # Check the Query Object
        for unsupported in ('aggregate', 'group', 'skip', 'limit'):
            if unsupported in mjp.query_object:
                raise InvalidQueryError('MongoSQL does not support `{}` for joined queries'
                                        .format(unsupported))

        # JOIN
        joined_query = query.join((mjp.relationship, mjp.target_model_aliased))

        # Nested MongoQuery
        query = mjp.nested_mongoquery \
            .from_query(joined_query) \
            .end().with_labels()

        # Done
        return query.options(
            as_relation.contains_eager(
                mjp.relationship,
                alias=mjp.target_model_aliased))

    def _load_relationship_with_filter__selectinload(self, query, as_relation, mjp):
        """ Load a relationship with a custom sort of selectinload() and filter it

            This technique will issue a second query, loading all the related entities separately, and populating
            the relation field with the results of that query.
            This is perhaps the most efficient technique available.

            See: https://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html#select-in-loading
        """
        raise NotImplementedError

        # TODO: can't we mimic the behavior of selectinload() here? Load all instances,
        #  gather their primary keys, make another query, populate their attributes...
        #  That's a lot of work, but the result should be fantastic!
        #  Moreover, I'm sure we can reuse a lot of code from selectinload(), just feed it with
        #  a query it does not expect.

    # endregion

    # Extra features

    def pluck_instance(self, instance):
        """ Pluck an sqlalchemy instance and make it into a dict -- for JSON output

            See MongoProject.pluck_instance()

            This method plucks relationships, and uses nested MongoQuery objects to pluck recursively

            :param instance: object
            :rtype: dict
        """
        ret = {}
        for mjp in self.mjps:
            # The relationship we're handling. It's been loaded.
            rel_name = mjp.relationship_name

            # Get property value
            value = getattr(instance, rel_name)

            # Alright, now `value` is the loaded relationship.
            # Now, it can be a list of related entities (mjp.uselist), or a single entity, or None
            # We don't care how to handle nested entities here, because the nested MongoQuery will do that.
            # Pluck
            if mjp.uselist:
                value = [mjp.nested_mongoquery.pluck_instance(e)
                         for e in value]
            else:
                if value is not None:
                    value = mjp.nested_mongoquery.pluck_instance(value)

            # Store
            ret[rel_name] = value
        return ret


class MongoJoinParams(object):
    """ All the information necessary for MongoQuery to build a join clause

        Because JOINs are complicated, we need a dataclass to transport the necessary information
        about it to the target MongoQuery procedure that will actually implement it.
    """

    def __init__(self,
                 model,
                 bags,
                 relationship_name,
                 relationship,
                 target_model,
                 target_model_aliased=None,
                 query_object=None,
                 nested_mongoquery=None):
        """ Values for joins

        :param model: The source model of this relationship
        :type model: sqlalchemy.ext.declarative.DeclarativeMeta
        :param bags: Model property bags
        :type bags: mongosql.bag.ModelPropertyBags
        :param relationship_name: Name of the relationship property
        :type relationship_name: str
        :param relationship: Relationship that is being joined
        :type relationship: sqlalchemy.orm.attributes.InstrumentedAttribute
        :param target_model: Target model of that relationship
        :type target_model: sqlalchemy.ext.declarative.DeclarativeMeta
        :param target_model_aliased: Target model, aliased
        :type target_model_aliased: sqlalchemy.orm.util.AliasedClass
        :param query_object: Query object dict for :meth:MongoQuery.query(). It can have more filters,
            joins, and whatnot.
        :type query_object: dict | None
        :param nested_mongoquery: Nested MongoQuery, initialized with all the aliases,
            and with `query_object` as its input.
        :type nested_mongoquery: mongosql.MongoQuery | None
        """
        self.model = model
        self.bags = bags

        self.relationship_name = relationship_name
        self.relationship = relationship

        self.target_model = target_model
        self.target_model_aliased = target_model_aliased

        self.uselist = relationship.property.uselist  # is relationship array?

        self.query_object = query_object or None  # remake it into None when an empty dict is given
        self.nested_mongoquery = nested_mongoquery

    @property
    def has_nested_query(self):
        """ Tell whether this MJP has a nested query

        :rtype: bool
        """
        return self.query_object is not None

    def __repr__(self):
        return '<MongoJoinParams(' \
               'model_name={0.bags.model_name}, ' \
               'relationship_name={0.relationship_name}, ' \
               'target_model={0.target_model}, ' \
               'query_object={0.query_object!r}, ' \
               ')>'.format(self)
















# region Magic for LEFT OUTER JOIN on a relationship with a custom ON clause

# Thanks to @vihtinsky <https://github.com/vihtinsky>: the guy who solved the puzzle.

# SqlAlchemy does not allow you to specify a custom ON-clause when joining to a relationship.
# Well, it does... by replacing the whole clause with the one you provided :)
# But in MongoSQL we needed to *add* more conditions to this clause.
# This is what these methods are here for: to permit a custom ON-clause.

from sqlalchemy import sql, inspection, __version__ as SA_VERSION

from sqlalchemy.orm.util import ORMAdapter
from sqlalchemy.sql import visitors
from sqlalchemy.sql.expression import and_


def _left_outer_join_with_filter(query, model, relation, related_alias, filter_clause):
    """ Generate a LEFT OUTER JOIN to a relationship with a custom ON-clause for filtering

    When join()ing relationships that we have to filter on, there's an issue that the resulting
    SQL will also filter the primary model that we join from.

    This function, implemented by @vihtinsky, builds a custom LEFT OUTER JOIN, putting the filtering
    condition into the ON clause.

    :param query: The query to join with
    :param model: The model to join from (or its alias)
    :param relation: The relationship to join to
    :param related_alias: An alias for the related model
    :param filter_clause: The custom ON-clause that will be ANDed to the primary clause
    :return: Query
    """
    # Let SqlAlchemy build the join pieces
    primaryjoin, secondaryjoin, source_selectable, \
    dest_selectable, secondary, target_adapter = _sa_create_joins(relation, model, related_alias)

    # Decide what to join to: the `right` side, and the ON clause
    if secondaryjoin is not None:  # no secondary join
        # note this is an inner join from secondary->right
        right = sql.join(secondary, related_alias, secondaryjoin)
    else:
        right = related_alias
    onclause = primaryjoin

    # Build our ON clause, and add the custom filter condition
    onclause = and_(_add_alias(onclause, relation, related_alias),
                    filter_clause)

    # Make a LEFT OUTER JOIN with the custom ON clause
    return query.outerjoin(right, onclause)


def _add_alias(join_clause, relationship, alias):
    """ Replace all references to columns from `relationship`, using an alias """
    # Search for references
    right_mapper = relationship.prop.mapper

    # Adapter: adapts columns to use the alias
    adapter = ORMAdapter(
        alias,
        equivalents=right_mapper
                    and right_mapper._equivalent_columns
                    or {},
    ).replace # that's a method!

    # Compile the JOIN clause by replacing references
    join_clause = visitors.replacement_traverse(join_clause, {},adapter)

    # Done
    return join_clause


def _sa_create_joins(relation, left, right):
    """ A helper to access the SqlAlchemy internal machinery that builds joins for relationships """

    # Left side of the join
    left_info = inspection.inspect(left)
    right_info = inspection.inspect(right)

    # Just a copy-paste from sqlalchemy
    adapt_to = right_info.selectable
    adapt_from = left_info.selectable

    # This is the magic sqlalchemy method that produces valid JOINs for the relationship
    if SA_VERSION.startswith('1.2'):
        # SA 1.2.x
        primaryjoin, secondaryjoin, source_selectable, \
        dest_selectable, secondary, target_adapter = \
            relation.prop._create_joins(
                source_selectable=adapt_from,
                source_polymorphic=True,
                dest_selectable=adapt_to,
                dest_polymorphic=True,
                of_type=right_info.mapper)
    elif SA_VERSION.startswith('1.3'):
        # SA 1.3.x: renamed `of_type` to `of_type_mapper`
        primaryjoin, secondaryjoin, source_selectable, \
        dest_selectable, secondary, target_adapter = \
            relation.prop._create_joins(
                source_selectable=adapt_from,
                dest_selectable=adapt_to,
                source_polymorphic=True,
                dest_polymorphic=True,
                of_type_mapper=right_info.mapper)

    return (
        primaryjoin,
        secondaryjoin,
        source_selectable,
        dest_selectable,
        secondary,
        target_adapter,
    )

# endregion
