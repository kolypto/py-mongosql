"""
### Join Operation
Joining corresponds to the `LEFT JOIN` part of an SQL query (although implemented as a separate query).

In the back-end database, the data is often kept in a *normalized form*:
items of different types are kept in different places.
This means that whenever you need a related item, you'll have to explicitly request it.

The Join operation lets you load those related items.

Please keep in mind that most relationships would be disabled on the back-end because of security concerns about
exposing sensitive data. Therefore, whenever a front-end developer needs to have a relationship loaded,
it has to be manually enabled on the back-end! Please feel free to ask.

Examples follow.

#### Syntax

* Array syntax.

    In its most simple form, all you need to do is just to provide the list of names of the relationships that you
    want to have loaded:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        join: ['user_profile', 'user_posts'],
    }))
    ```

* String syntax.

    List of relationships, separated by whitespace:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        join: 'user_profile user_posts',
    }))
    ```

* Object syntax.

    This syntax offers you great flexibility: with a nested Query Object, it is now posible to apply operations
    to related entities: select just a few fields (projection), sort it, filter it, even limit it!

    The nested Query Object supports projections, sorting, filtering, even joining further relations, and
    limiting the number of related entities that are loaded!

    In this object syntax, the object is an embedded Query Object. For instance:

    ```javascript
    $.get('/api/user?query=' + JSON.stringify({
        join: {
            // Load related 'posts'
            posts: {
                filter: { rating: { $gte: 4.0 } },  // Only load posts with raing > 4.0
                sort: ['date-'],  // newest first
                skip: 0,  // first page
                limit: 100,  // 100 per page
            },

            // Load another relationship
            'comments': null,  # No specific options, just load
            }
        }
    }))
    ```

    Note that `null` can be used to load a relationship without custom querying.
"""


import json
from types import SimpleNamespace

from sqlalchemy import exc as sa_exc
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

    def __init__(self, model, bags, allowed_relations=None, banned_relations=None, raiseload_rel=False, legacy_fields=None):
        """ Init a join expression

        :param model: Sqlalchemy model to work with
        :param bags: Model bags
        :param allowed_relations: List of relations that can be joined
        :param banned_relations: List of relations that can't be joined to
        :param raiseload_rel: Install a raiseload() option on all relations not explicitly loaded.
            This is a performance safeguard for the cases when your code might use them.
        """
        super(MongoJoin, self).__init__(model, bags)

        # Security
        if allowed_relations is not None and banned_relations is not None:
            raise ValueError('Cannot use both `allowed_relations` and `banned_relations`')
        elif allowed_relations is not None:
            self.allowed_relations = set(allowed_relations)
        elif banned_relations is not None:
            self.allowed_relations = self.bags.relations.names - set(banned_relations)
        else:
            self.allowed_relations = None

        # Raiseload?
        self.raiseload_rel = raiseload_rel

        # Legacy
        self.legacy_fields = frozenset(legacy_fields or ())
        self.legacy_fields_not_faked = self.legacy_fields - self.bags.all_names  # legacy_fields not faked as a @property

        # Use LEFT_JOIN strategy only once
        self._used_up_left_join_strategy = False

        # Validate
        if self.allowed_relations:
            self.validate_properties(self.allowed_relations, where='join:allowed_relations')

        # On input
        # type: dict
        self.relations = None
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
                raise DisabledError('Join: joining is disabled for relationship `{}.{}`'
                                    .format(self.bags.model_name, relation_name))
        # Yield it
        return relation

    def validate_properties(self, prop_names, bag=None, where=None):
        # Use the same logic, but remake the error into InvalidRelationError
        try:
            return super(MongoJoin, self).validate_properties(prop_names, bag, where)
        except InvalidColumnError as e:
            raise InvalidRelationError(e.model, e.column_name, e.where)

    def input(self, relations):
        assert self.mongoquery is not None, 'MongoJoin has to be coupled with a MongoQuery object. ' \
                                            'Call with_mongoquery() on it'
        super(MongoJoin, self).input(relations)
        self.relations, self.mjps = self._input_process(relations)
        return self

    def _input_process(self, relations):
        """ Process the input Query Object and produce a list of MJPs

            :returns: (dict, list[MongoJoinParams])
        """
        # Validation
        if not relations:
            relations = {}
        elif isinstance(relations, str):
            relations = {relname: None for relname in relations.split()}
        elif isinstance(relations, (list, tuple)):
            relations = {relname: None for relname in relations}
        elif isinstance(relations, dict):
            relations = relations
        else:
            raise InvalidQueryError('Join must be one of: null, string, array, object;'
                                    '{type} provided'.format(type=type(relations)))

        self.validate_properties(set(relations.keys()) - self.legacy_fields)

        # Go over all relationships and simply build MJP objects that will carry the necessary
        # information to the Query on the outside, which will use those MJP objects to handle the
        # actual joining process
        mjp_list = []
        for relation_name, query_object in relations.items():
            # Add an ignored object for legacy_fields
            if relation_name in self.legacy_fields:
                mjp = LegacyMongoJoinParams(
                    relationship_name=relation_name,
                    query_object=query_object or None
                )
                mjp_list.append(mjp)
                continue

            # Get the relationship and its target model
            rel = self._get_relation_securely(relation_name)
            target_model = self.bags.relations.get_target_model(relation_name)
            target_model_aliased = aliased(rel)  # aliased(rel) and aliased(target_model) is the same thing

            # Prepare the nested MongoQuery
            # We do it here so that all validation errors come on input()
            nested_mongoquery = self.mongoquery._get_nested_mongoquery(relation_name)

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
                query_object=query_object or None,  # force falsy values to `None`
                parent_mongoquery=self.mongoquery,
                nested_mongoquery=nested_mongoquery,
            )

            # Choose the loading strategy
            mjp.loading_strategy = self._choose_relationship_loading_strategy(mjp)

            # There's a bug in the LEFT_JOIN strategy that prevents it from functioning correctly
            # if there are two relationships using left join and a LIMIT in the same clause.
            # I'm not going to fix it; instead, I switch to SELECTINQUERY
            # And we don't care whether there's a limit; just don't let two LEFT JOINs happen.
            if mjp.loading_strategy in (self.RELSTRATEGY_LEFT_JOIN, self.RELSTRATEGY_EAGERLOAD):
                # Switch to SELECTINQUERY if this MongoJoin has already used LEFT_JOIN once
                if self._used_up_left_join_strategy:
                    mjp.loading_strategy = self.RELSTRATEGY_SELECTINQUERY

                # Don't let this MongoJoin use a LEFT JOIN again
                self._used_up_left_join_strategy = True

            # Unfortunately, a MongoQuery has to be aliased() upfront, before query() is called.
            # Therefore, we have to do it right now.
            # However, some relationship loading strategies want aliased(), some do not.
            # selectinquery() is the only one that does not want no aliases.
            if mjp.loading_strategy == self.RELSTRATEGY_SELECTINQUERY:
                # selectinquery() does not want aliases, so we don't do it.
                # However!
                # After a lot of pain, it was discovered that even though the second query that selectinquery()
                # issues is a separate query, it *still* has to have a proper Load() interface chaining from
                # the original relationship.
                # Let's do it
                mjp.nested_mongoquery.as_relation_of(self.mongoquery, mjp.relationship)
            else:
                # Everyone else wants an alias.
                # as_relation_of() and aliased() it property
                mjp.nested_mongoquery = mjp.nested_mongoquery \
                    .as_relation_of(mjp.parent_mongoquery, mjp.relationship) \
                    .aliased(mjp.target_model_aliased)

            # Nested MongoQuery: input the query object
            # We do it here, not later, so that all validation procedures take place and throw their exceptions early on
            mjp.nested_mongoquery.query(**mjp.query_object or {})

            # Add the newly constructed MJP to the list
            mjp_list.append(mjp)

        return relations, mjp_list

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
            if not isinstance(mjp, LegacyMongoJoinParams):
                query = self._load_relationship(query, as_relation, mjp)

        # Put a raiseload_rel() on every other relationship!
        if self.raiseload_rel:
            query = query.options(as_relation.raiseload('*'))

        return query

    # region Relationship Loading Strategies

    # Turn on the modern option of loading relationships with selectinquery().
    # selectinquery() is experimental; therefore, it can be disabled
    ENABLED_EXPERIMENTAL_SELECTINQUERY = True

    # Constants for relationship loading strategy
    RELSTRATEGY_EAGERLOAD = 'EAGERLOAD'
    RELSTRATEGY_LEFT_JOIN = 'LJOIN'
    RELSTRATEGY_JOINF = 'JOINF'
    RELSTRATEGY_SELECTINQUERY = 'SELECTINQUERY'

    def _choose_relationship_loading_strategy(self, mjp):
        """ Make a decision on how to load the relationship.

        :type mjp: MongoJoinParams
        :returns: str Relationship loading strategy
        """
        # The user has requested a relationship, and here we decide how to load it.
        # There are two major cases to consider:
        # A. No nested Query Object.
        #    In this case, there's no filtering, projection, or anything, installed on the query.
        #    We can just load it like we always do.
        # B. There is a nested Query Object.
        #    The user has requested a relationship, and he also wants to filter it, use projection, and perhaps,
        #    load even more relationships.
        #    In this case, we will have to use a nested MongoQuery object to generate that query for us.

        # Now, how do we do it?
        # Let's consider the options.

        # 1. 𝗦𝗾𝗹𝗔𝗹𝗰𝗵𝗲𝗺𝘆'𝘀 𝗲𝗮𝗴𝗲𝗿 𝗹𝗼𝗮𝗱𝗶𝗻𝗴: 𝗷𝗼𝗶𝗻𝗲𝗱𝗹𝗼𝗮𝗱(), 𝘀𝗲𝗹𝗲𝗰𝘁𝗶𝗻𝗹𝗼𝗮𝗱().
        #    Obviously, it only works for relationships with no nested Query Objects:
        #    because SqlAlchemy simply cannot filter related entities!
        #    So that's our choice for scenario A: no nested query.
        # 2. 𝗝𝗢𝗜𝗡
        #    Select from the primary entity, and join() the related entity to it, then use contains_eager().
        #    All filters, projections, and ordering will be applied to the whole query.
        #    This approach will actually 𝗱𝗶𝘀𝘁𝗼𝗿𝘁 𝘁𝗵𝗲 𝗿𝗲𝘀𝘂𝗹𝘁𝘀 𝗼𝗳 𝘁𝗵𝗲 𝗽𝗿𝗶𝗺𝗮𝗿𝘆 𝗾𝘂𝗲𝗿𝘆:
        #    because when the primary table has nothing to JOIN to... the row is dropped.
        #    Imagine: users JOIN articles ; and there's a user with no rows in articles. Oops.
        #    So this method can't be used for loading relationships.
        # 3. 𝗟𝗘𝗙𝗧 𝗢𝗨𝗧𝗘𝗥 𝗝𝗢𝗜𝗡
        #    Use LEFT OUTER JOIN to join the related table to the primary one.
        #    This solves the issue we had with the `JOIN` case: when the primary instance has no related rows,
        #    it will still remain. It won't disappear: it will be a row of NULLs. Perfect.
        #    However, in this case there is a different issue: when there is a filter on the related entity,
        #    you cannot just put in into the WHERE clause. Because any condition in the WHERE clause will fail
        #    to match the NULLs!
        #    There may be two solutions to this problem.
        #    3.1. Put the filtering condition into the ON clause.
        #         Example:
        #           SELECT *
        #           FROM users LEFT OUTER JOIN articles
        #               ON users.id = articles.author_id
        #               AND articles.rating > 0.5
        #    3.2. Put the filtering condition into the WHERE clause, OR'ed with the possibility of having a NULL row.
        #         Example:
        #           SELECT *
        #           FROM users LEFT OUTER JOIN articles
        #               ON users.id = articles.author_id
        #           WHERE articles.ratikng > 0.5 OR articles.id IS NULL
        #    The limitations of this approach are:
        #    * For every row in the primary table, you may have multiple rows in the related table.
        #       This transmits more data over the socket connection, forces sqlalchemy to do deduplication,
        #       and also spoils the total number of rows: you just can't count them!
        # 3. 𝗦𝘂𝗯𝗾𝘂𝗲𝗿𝘆
        #    We can make the nested MongoQuery as a subquery, and join to it.
        #    The subquery will select & filter all the relevant related entities, even LIMIT them,
        #    and then our primary query can just join to it.
        #    In this case, the nested condition will be isolated inside the subquery
        #    and will not distort the results of primary query.
        #    This method is not too different from JOINing, so it was not even considered.
        # 4. 𝘀𝗲𝗹𝗲𝗰𝘁𝗶𝗻𝗾𝘂𝗲𝗿𝘆()
        #    One evening I was wondering at selectinload() and dreaming: if it only could support custom filtering!
        #    This wonderful method runs a second query that loads related entities ; such a beauty!
        #    What if I can alter that query, and teach it to do projections, filtering, even grouping, perhaps?
        #    That's how selectinquery() was born: a loader option that lets you customize the query.
        #    This loading strategy injects a nested MongoSql query into the one generated by selectinload(),
        #    and uses its internal machinery to load related entities.
        #    This is currently the best method available for one-to-many and many-to-many relationships.

        # Now, how do we load relationships?
        # It depends.
        # If there is no nested query, we don't need no custom stuff: just use the built-in sqlalchemy machinery.
        #   It will use joinedload() for one-to-one relationships;
        #   It will use selectinload() for `uselist` relationships.
        # If there is a nested query, however:
        #   It will use LEFT OUTER JOIN for one-to-one relationships
        #   It will use selectinquery() for `uselist` relationships,
        #       but it will fall back to LEFT OUTER JOIN, if selectinquery() is disabled.

        # Implement this logic:
        if mjp.has_nested_query:
            # Has a Query Object
            # SqlAlchemy can't handle it: have to use our custom methods.
            # Depending on the type of relationship:
            if mjp.uselist:
                # x-to-many relationship:
                if self.ENABLED_EXPERIMENTAL_SELECTINQUERY:
                    # selectinquery() is experimental; therefore, it can be disabled
                    return self.RELSTRATEGY_SELECTINQUERY
                else:
                    # fall back, when selectinquery() is disabled
                    return self.RELSTRATEGY_LEFT_JOIN
            else:
                # one-to-one relationship:
                return self.RELSTRATEGY_LEFT_JOIN
        else:
            return self.RELSTRATEGY_EAGERLOAD

    def _load_relationship(self, query, as_relation, mjp):
        """ Load the relationship using the chosen strategy """
        return {
            # List of strategies mapped to their handler methods
            self.RELSTRATEGY_EAGERLOAD: self._load_relationship_sqlalchemy_eagerload,
            self.RELSTRATEGY_LEFT_JOIN: self._load_relationship_with_filter__left_join,
            self.RELSTRATEGY_JOINF: self._load_relationship_with_filter__joinf,
            self.RELSTRATEGY_SELECTINQUERY: self._load_relationship_with_filter__selectinquery,
        }[mjp.loading_strategy](query, as_relation, mjp)  # use the method

    def _load_relationship_sqlalchemy_eagerload(self, query, as_relation, mjp):
        """ Load a relationship using sqlalchemy's eager loading.

            This method just uses SqlAlchemy's eager loading.
            It's only applicable when there is no nested query present, because SqlAlchemy can't filter relationships
            loaded with options(): it just gives them all.

            :type query: sqlalchemy.orm.Query
            :type as_relation: Load
            :type mjp: MongoJoinParams
        """
        assert not mjp.has_nested_query, 'Cannot use this strategy when a nested query is present'
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

        # Run nested MongoQuery
        # It's already been alias()ed and as_relation_from()ed
        # We still have to let the nested MongoQuery run its business
        # It may be installing projections even when there's no Query Object:
        # because there are default settings, and sometimes the user does not get what he wants, but what we want :)
        query = mjp.nested_mongoquery \
            .from_query(query).end()

        # Since there's no Query Object, there's no projection nor join provided.
        # This means that the user does not want sub-relations, so we don't load them.
        if self.raiseload_rel:
            rel_load.raiseload('*')
        else:
            rel_load.lazyload('*')  # deferred loading upon request

        # Done here
        return query.options(rel_load)

    def _load_relationship_with_filter__left_join(self, query, as_relation, mjp):
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

            :type query: sqlalchemy.orm.Query
            :type as_relation: Load
            :type mjp: MongoJoinParams
        """
        # Check the Query Object
        if mjp.query_object:
            for unsupported in ('aggregate', 'group'):
                if unsupported in mjp.query_object:
                    raise InvalidQueryError('MongoSQL does not support `{}` for joined queries (relationship={}, strategy={})'
                                            .format(unsupported, mjp.relationship_name, mjp.loading_strategy))
            if 'skip' in mjp.query_object or 'limit' in mjp.query_object:
                raise InvalidQueryError('MongoSQL does not support `skip` or `limit` for this kind of `join` (relationship={}, strategy={})'
                                        .format(mjp.relationship_name, mjp.loading_strategy))
        if mjp.nested_mongoquery:
            if mjp.nested_mongoquery.handler_limit.max_items:
                raise ValueError('MongoSQL does not support `max_items` for this kind of relationship (relationship={}, strategy={})'
                                 .format(mjp.relationship_name, mjp.loading_strategy))


        # Handle the case when the query has a LIMIT, and sqlalchemy won't do a JOIN to it
        query = self._join__wrap_query_with_subquery_to_overcome_LIMIT_issues(query, mjp, as_relation)

        # If our source model is aliased, we have to use its alias in the query
        # self.model is that very thing: it's aliased, if we're aliased()
        source_model_aliased = self.model

        # Get the nested MongoQuery
        # It's already been alias()ed and as_relation_from()ed
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
        # we have to make sure that the same condition won't be applied again.
        nested_mq.handler_filter.skip_this_handler = True

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

            :type query: Query
            :type as_relation: Load
            :type mjp: MongoJoinParams
        """
        # Check the Query Object
        if mjp.query_object:
            for unsupported in ('aggregate', 'group', 'skip', 'limit'):
                if unsupported in mjp.query_object:
                    raise InvalidQueryError('MongoSQL does not support `{}` for queries joined with `joinf`'
                                            .format(unsupported))
        if mjp.nested_mongoquery:
            if mjp.nested_mongoquery.handler_limit.max_items:
                raise ValueError('MongoSQL does not support `max_items` for this kind of relationship (relationship={}, strategy={})'
                                 .format(mjp.relationship_name, mjp.loading_strategy))

        # Handle the case when the query has a LIMIT, and sqlalchemy won't do a JOIN to it
        query = self._join__wrap_query_with_subquery_to_overcome_LIMIT_issues(query, mjp, as_relation)

        # JOIN
        joined_query = query.join((mjp.relationship, mjp.target_model_aliased))

        # Run nested MongoQuery
        # It's already been alias()ed and as_relation_from()ed
        query = mjp.nested_mongoquery \
            .from_query(joined_query) \
            .end().with_labels()

        # Done
        return query.options(
            as_relation.contains_eager(
                mjp.relationship,
                alias=mjp.target_model_aliased))

    def _load_relationship_with_filter__selectinquery(self, query, as_relation, mjp):
        """ Load a relationship with a custom sort of selectinload() and filter it

            This technique will issue a second query, loading all the related entities separately, and populating
            the relation field with the results of that query.
            This is perhaps the most efficient technique available, and the most flexible.

            See: https://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html#select-in-loading

            :type query: sqlalchemy.orm.Query
            :type as_relation: Load
            :type mjp: MongoJoinParams
        """
        # Check the Query Object
        if mjp.query_object:
            for unsupported in ('aggregate', 'group'):
                if unsupported in mjp.query_object:
                    raise InvalidQueryError('MongoSQL does not support `{}` for joined queries (relationship={}, strategy={})'
                                            .format(unsupported, mjp.relationship_name, mjp.loading_strategy))

        # It's not being loaded as a relation anymore ; it' loaded in a separate query.
        # Thus, we need it un-aliased().
        nested_mq = mjp.nested_mongoquery

        # Prepare the loader option

        # Tell the nested MongoLimit handler that it has to apply a smart limit to the query.
        # It has to use a window function over a partition groped by the foreign key.
        # Therefore, it has to know which foreign key to use.
        # Get the list of foreign key columns for this relationship
        relation_fk = mjp.relationship.property.remote_side
        # Give them to the MongoLimit handler
        nested_mq.handler_limit.limit_groups_over_columns(relation_fk)

        # Just set the option. That's it :)
        return query.options(
            as_relation.selectinquery(
                relationship=mjp.relationship,
                alter_query=lambda q: nested_mq.from_query(q).end(),
                cache_key=get_mongoquery_cache_key(query, nested_mq),  # cached, yes!
            )
        )

    def _join__wrap_query_with_subquery_to_overcome_LIMIT_issues(self, query, mjp, as_relation):
        """ SqlAlchemy would refuse to do Query.join() when it has a LIMIT on it already:

            sqlalchemy.exc.InvalidRequestError: Query._join() being called on a Query which already has LIMIT or OFFSET applied.
            To modify the row-limited results of a  Query, call from_self() first.
            Otherwise, call _join() before limit() or offset() are applied.

            This method is used for both `join` and `joinf`: for this reason, it's moved to a separate method.
        """
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
            #
            # undefer() every column that participates in the ORDER BY
            # We're adding extra columns to the result set, but that's alright.
            # I've seen some really custom code raise weird errors if we don't. So let it be.
            query = query.options(sort_handler.undefer_columns_involved_in_sorting(as_relation))

            # We also have to undefer any columns that participate in this relationship
            # If foreign keys are deferred, SqlAlchemy won't be able to adapt the join condition properly:
            # it will use the original table name (not the subquery alias), which results in an invalid query.
            local_columns = mjp.relationship.property.local_columns
            query = query.options(*[as_relation.undefer(column.key)
                                    for column in local_columns])

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

                # Here, we used to undo undefer()ed columns and restore the query to its original state, but we don't
                # do it anymore: I've seen weird bugs because of this!

        return query

    # endregion

    # Extra features

    @property
    def projection(self):
        """ Get a projection-like dict from the join handler

            Since "join" decides which properties to load and which not to, it behaves like a sort of projection.
            This property will generate a dict {'relname': 1} for you.
            It may be useful to have a clear picture about what's loaded and what isn't.

            Example:

                MongoQuery(User).query(join={'articles': ...}).handler_join.projection
                #-> {'articles': 1}

            :rtype: dict
        """
        return {mjp.relationship_name: 1
                for mjp in self.mjps
                if not mjp.quietly_included}

    def get_projection_tree(self):
        """ Get a projection-like dict that will also have nested dictionaries for nested projections

            When a relationship has a nested Query Object, it will be mapped to another dict.
            Example:

                MongoQuery(User).query(join={'articles': dict(project=('id',))}).handler_join.projection
                #-> {'articles': {'id': 1}}

            This is mainly useful for debugging nested Query Objects.
            :rtype: dict
        """
        return {mjp.relationship_name: mjp.nested_mongoquery.get_projection_tree()
                for mjp in self.mjps
                if not mjp.quietly_included}

    def get_full_projection_tree(self):
        """ Get a projection tree where every column is mapped to either 1 or 0 """
        return {mjp.relationship_name: mjp.nested_mongoquery.get_full_projection_tree()
                for mjp in self.mjps
                if not mjp.quietly_included}

    def get_full_projection(self):
        """ Get a full projection-like dict from the join handler

            It will include every known relationship, mapped either to 1 or to 0.
            Example:

                MongoQuery(User).query(join={'articles': ...}).handler_join.projection
                #-> {'articles': 1, 'comments': 0}

            :rtype: dict
        """
        projection = self.projection
        all_names = set([*self.bags.relations.names, *self.legacy_fields])
        return {relation_name: projection.get(relation_name, 0)
                for relation_name in all_names}

    def merge(self, relations, quietly=False, strict=False):
        """ Add another relationship to be eagerly loaded.

            This enables you to load additional relationships, even after the Query Object has been processed.
            Note that it only lets you load these relationships in a simple fashion ; no nested Queries are supported.

            Furthermore, if a relationship has already been loaded via input(),
            and it conflicts with the current relationship, you will get an error.
            A 'conflict' is when either one of these relationships contains anything but 'project', 'sort', 'join',
            because then they cannot be merged. Even with 'sort', only either of them may have it, but not both.

            :param relations: Relationships to load eagerly
            :type relations: dict | list
            :param quietly: Whether to include the new relations and projections quietly:
                that is, without changing the results of `self.projection` and `self.pluck_instance()`.
                See MongoQuery.ensure_loaded() for more info.
            :type quietly: bool
            :param strict: Refuse to join when a filter is present (see ensure_loaded)
            :type strict: bool
            :rtype: MongoJoin
            :raises InvalidQueryError: Conflicting query objects
        """
        assert self.input_received, 'Can only use merge() when the input() has already been received'

        # Process the input
        relations, mjps = self._input_process(relations)

        # Current MJPs
        current_mjps = {mjp.relationship_name: mjp for mjp in self.mjps}

        # Configure strict mode
        if strict:
            merge_allowed_keys = {'project', 'join', 'sort'}
            strict_mode_str = 'strict mode'
        else:
            merge_allowed_keys = {'project', 'join', 'sort', 'filter'}
            strict_mode_str = 'non-strict mode'

        # Helpers
        is_mjp_simple = lambda mjp: mjp is None \
                                    or not mjp.query_object or \
                                    set(mjp.query_object.keys()) <= merge_allowed_keys

        mjp_has_something = lambda mjp, key: mjp is not None and \
                                   mjp.query_object and \
                                   key in mjp.query_object
        mjp_has_sort = lambda mjp: mjp_has_something(mjp, 'sort')
        mjp_has_filter = lambda mjp: mjp_has_something(mjp, 'filter')


        # Merge both dicts and MJPs
        for mjp in mjps:
            relation_name = mjp.relationship_name

            # Find a matching MJP, if there even is one
            current_mjp = current_mjps.get(relation_name, None)  # type: MongoJoinParams

            # Test if the two MJPs are compatible
            # Let me explain.
            # The goal of this merging is to provide a superset of results that is compatible with the original request.
            #
            # Two MJPs won't be compatible if either MJP contains a filter:
            # just imagine that the API user expects only a limited number of entities,
            # while the application expects the relationship to be loaded completely.
            # Or vice versa: the API expects a filtered result, but the application has loaded them all.
            #
            # Therefore, to make sure that both requests are satisfied, we impose a limitation:
            # you can only merge two MJPs when neither of them contains:
            #       filter, group, aggregate, joinf, limit, count
            # They can, however, contain:
            #       project, join. sort
            #       sort: either, but not both
            if not is_mjp_simple(mjp):
                raise InvalidQueryError("You can only merge() a simple relationship, "
                                        "whose Query Object is limited to {} ({}); "
                                        "Your relationship '{}' Query Object has more than that."
                                        .format(merge_allowed_keys, strict_mode_str, relation_name))
            if not is_mjp_simple(current_mjp):
                raise InvalidQueryError("You can only merge() to simple relationships, "
                                        "whose Query Objects is limited to {} ({}); "
                                        "Relationship '{}' has already been loaded with advanced features. "
                                        "Cannot merge to it."
                                        .format(merge_allowed_keys, strict_mode_str, relation_name))

            if strict:
                if mjp_has_sort(mjp) and mjp_has_sort(current_mjp):
                    raise InvalidQueryError("You can only merge() when one of the Query Objects has 'sort', but not both.")

            # If there was no relationship - just add it
            if current_mjp is None:
                # Easy
                self.relations[relation_name] = mjp.query_object
                self.mjps.append(mjp)

                # Exclude from plucking
                if quietly:
                    mjp.quietly_included = True
            else:
                # Have to merge them
                # Merge projections
                current_mjp.nested_mongoquery.handler_project.merge(
                    mjp.nested_mongoquery.handler_project.projection,
                    quietly=quietly
                )

                # Merge joins
                current_mjp.nested_mongoquery.handler_join.merge(
                    mjp.nested_mongoquery.handler_join.relations,
                    quietly=quietly
                )

                if not strict:
                    # Merge filters
                    current_mjp.nested_mongoquery.handler_filter.merge(
                        mjp.nested_mongoquery.handler_filter.input_value
                    )

                    # Merge sorting
                    current_mjp.nested_mongoquery.handler_sort.merge(
                        mjp.nested_mongoquery.handler_sort.sort_spec
                    )

                # Merge relations dict, and their keys
                if self.relations[relation_name] is None:
                    self.relations[relation_name] = {}
                self.relations[relation_name]['project'] = current_mjp.nested_mongoquery.handler_project.projection
                self.relations[relation_name]['join'] = current_mjp.nested_mongoquery.handler_join.relations
                if not strict:
                    self.relations[relation_name]['sort'] = current_mjp.nested_mongoquery.handler_sort.sort_spec
                    filt = {}
                    filt.update(mjp.nested_mongoquery.handler_filter.input_value or {})
                    filt.update(current_mjp.nested_mongoquery.handler_filter.input_value or {})
                    self.relations[relation_name]['filter'] = filt

            # We don't have to re-initialize MongoQuery or anything, because we only support two handlers:
            # join, and project, and both have this 'merge' method

        # Done
        return self

    def get_final_input_value(self):
        return {
            mjp.relationship_name: mjp.nested_mongoquery.get_final_query_object()
            for mjp in self.mjps
        }

    def __contains__(self, name):
        """ Test whether a relationship name is going to be eagerly loaded (by name)

        :type item: str
        """
        return name in self.relations

    def pluck_instance(self, instance):
        """ Pluck an sqlalchemy instance and make it into a dict -- for JSON output

            See MongoProject.pluck_instance()

            This method plucks relationships, and uses nested MongoQuery objects to pluck recursively

            :param instance: object
            :rtype: dict
        """
        ret = {}
        for mjp in self.mjps:
            # Do not include quietly-included fields
            if mjp.quietly_included:
                continue
            # Skip legacy fields that are not backed by a @property
            if mjp.relationship_name in self.legacy_fields_not_faked:
                continue

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


class MongoJoinParams:
    """ All the information necessary for MongoQuery to build a join clause

        Because JOINs are complicated, we need a dataclass to transport the necessary information
        about it to the target MongoQuery procedure that will actually implement it.
    """

    __slots__ = ('model', 'bags',
                 'relationship_name', 'relationship',
                 'target_model', 'target_model_aliased',
                 'query_object',
                 'parent_mongoquery',
                 'nested_mongoquery',
                 'uselist', 'loading_strategy',
                 'quietly_included')

    def __init__(self,
                 model,
                 bags,
                 relationship_name,
                 relationship,
                 target_model,
                 target_model_aliased,
                 query_object,
                 parent_mongoquery,
                 nested_mongoquery):
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
        :param query_object: Query object dict for :meth:MongoQuery.query(). It can have more filters, joins, and whatnot.
        :type query_object: dict | None
        :param parent_mongoquery: Parent MongoQuery
        :type parent_mongoquery: mongosql.query.MongoQuery
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
        self.parent_mongoquery = parent_mongoquery
        self.nested_mongoquery = nested_mongoquery

        self.loading_strategy = None  # will be added later

        # Whether to include this field into get_full_projection() and pluck_instance()
        # `True` is for the relationships that were officially requested by the user
        # `False` is for the relationships that were quietly loaded by the application, but should be excluded from the
        # response because the API user has not requested it.
        self.quietly_included = False

    @property
    def has_nested_query(self):
        """ Tell whether this MJP has a nested query

        :rtype: bool
        """
        # A nested query may result in two cases:
        # 1. There is a Query Object
        # 2. There is a setting that works like a Query Object

        # Nested query will happen in case of a Query Object
        if self.query_object is not None:
            return self.query_object

        # Some settings will require a nested query to make sense
        nmq = self.nested_mongoquery
        if nmq:
            return any((
                nmq.handler_limit.max_items is not None,
                nmq.handler_filter.force_filter is not None,
            ))

        # No nested query in all other cases
        return False

    def __repr__(self):
        return '<MongoJoinParams(' \
               'model_name={0.bags.model_name}, ' \
               'relationship_name={0.relationship_name}, ' \
               'loading_strategy={0.loading_strategy}, ' \
               'target_model={0.target_model.__name__}, ' \
               'target_model_aliased={0.target_model_aliased}, ' \
               'query_object={0.query_object!r}, ' \
               ')>'.format(self)


class LegacyMongoJoinParams:
    """ An MJP object that's actually ignored. It's used for legacy_fields """

    def __init__(self, relationship_name, query_object):
        self.relationship_name = relationship_name
        self.query_object = query_object

        # Follow the protocol: fake some fields
        self.quietly_included = False
        self.nested_mongoquery = SimpleNamespace()  # empty object
        self.nested_mongoquery.get_final_query_object = lambda: self.query_object
        self.nested_mongoquery.get_projection_tree = lambda: 1
        self.nested_mongoquery.get_full_projection_tree = lambda: 1

    def __repr__(self):
        return f'<LegacyMongoJoinParams(relationship_name={self.relationship_name}, query_object={self.query_object!r})>'


# region Join helpers

class JSONCacheKeyEncoder(json.JSONEncoder):
    """ A JSON encoder that can encode everything

        Everything that it can't encode, it encodes using a repr()
    """
    def default(self, o):
        return repr(o)


def get_mongoquery_cache_key(query, nested_mongoquery):
    """ Get the hash key for the current query

        This must include the original SqlAlchemy's query hash, plus,
        a hash of every MongoQuery object down to the current one
    """
    # First, get some sort of hash from the sqlalchemy query
    # First, compile the query into a string. That's the first part of our key.
    if query.session:
        # Get the current dialect from the session's engine and use it for compilation
        dialect = query.session.bind.dialect
        stmt_compiled = query.statement.compile(dialect=dialect)
    else:
        # When there's no session, try to compile it without a dialect.
        # This may throw errors about DB-specific types that are counter-intuitive, so we have to explain them to the user
        try:
            stmt_compiled = query.statement.compile()
        except sa_exc.UnsupportedCompilationError as e:
            raise RuntimeError(
                "Failed to compile an SQL statement. "
                "This is likely because your Query object is not bound to a Session, "
                "and SqlAlchemy doesn't know the SQL dialect you're using. "
                "Please use Query.with_session() or MongoQuery.with_session() "
                "so that the Query you're using is bound to an SqlAlchemy Session"
            ) from e
    # The second part of that key are the values. But we do not compile them into the query, but it turns out that
    # keeping them as (stmt, json-encoded params) is more robust.
    # stmt_compiled = query.statement.compile(compile_kwargs={"literal_binds": True})
    q_hash = (stmt_compiled.string, stmt_compiled.params)
    q_hash = json.dumps(q_hash, cls=JSONCacheKeyEncoder, sort_keys=True)

    # Second, get a hash of the MongoQuery
    # However, because we have nested queries, we'll have to take a hash of every single one of them
    # while going upwards. Otherwise, cache collisions are possible
    mq = nested_mongoquery
    mq_hash = []
    while mq is not None:
        # QueryObject on this level
        mq_hash.append(mq.input_value)
        # Go up?
        mq = mq._parent_mongoquery
    mq_hash = json.dumps(mq_hash, cls=JSONCacheKeyEncoder, sort_keys=True)

    # Combine them all
    return q_hash + '/' + mq_hash


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
    onclause = and_(onclause, #_add_alias(onclause, relation, related_alias),
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
    else:
        raise RuntimeError('Unsupported SqlAlchemy version! Expected 1.2.x or 1.3.x')

    return (
        primaryjoin,
        secondaryjoin,
        source_selectable,
        dest_selectable,
        secondary,
        target_adapter,
    )

# endregion

# endregion
