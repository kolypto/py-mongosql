from __future__ import absolute_import
from sqlalchemy.orm import aliased

from .base import _MongoQueryStatementBase
from ..bag import ModelPropertyBags, CombinedBag
from ..exc import InvalidQueryError, InvalidColumnError, InvalidRelationError


class MongoJoinParams(object):
    """ All the information necessary for MongoQuery to build a join clause

        Because JOINs are complicated, we need a dataclass to transport the necessary information
        about it to the target MongoQuery procedure that will actually implement it.
    """

    def __init__(self,
                 options,
                 model,
                 bags,
                 relationship_name,
                 relationship,
                 target_model,
                 target_model_aliased=None,
                 query_object=None,
                 additional_filter=None):
        """ Values for joins

        :param options: Additional query options
        :type options: Sequence[sqlalchemy.orm.Load] | None
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
        :param additional_filter: Callable(query) to produce additional filtering
        :type additional_filter: callable | None
        """
        self.options = options

        self.model = model
        self.bags = bags

        self.relationship_name = relationship_name
        self.relationship = relationship

        self.target_model = target_model
        self.target_model_aliased = target_model_aliased

        self.query_object = query_object

        self.additional_filter = additional_filter

    def __repr__(self):
        return '<MongoJoinParams(' \
               'model_name={0.bags.model_name}, ' \
               'relationship_name={0.relationship_name}, ' \
               'target_model={0.target_model}, ' \
               'query_object={0.query_object!r}, ' \
               'additional_filter={0.additional_filter!r}' \
               ')>'.format(self)


class MongoJoin(_MongoQueryStatementBase):
    """ Joining relations (eager load)

        - List of relation names
        - Dict: { relation-name: query-dict } for :meth:MongoQuery.query
    """

    query_object_section_name = 'join'

    def __init__(self, model, allowed_relations=None, banned_relations=None):
        """ Init a join expression

        :param allowed_relations: List of relations that can be joined
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
                raise InvalidQueryError('Join: joining is disabled for relationship `{}`'
                                        .format(relation_name))
        # Yield it
        return relation

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
            raise InvalidQueryError('Join must be one of: None, list, dict')

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

            # Start preparing the MJP: MongoJoinParams.
            mjp = MongoJoinParams(
                # Options are None unless there's a query to execute
                options=None,
                model=self.bags.model,
                bags=self.bags,
                relationship_name=relation_name,
                relationship=rel,
                target_model=target_model,
                # Got to use an alias because when there are two relationships to the same model,
                # it would fail because of ambiguity
                target_model_aliased=aliased(target_model),
                query_object=query_object,  # TODO: query_object has to be input()ed here with a MongoQuery!
                additional_filter=None
            )

            # Add the newly constructed MJP to the list
            mjp_list.append(mjp)

        self.mjps = mjp_list
        return self

    def compile_options(self, as_relation):
        # TODO: a feature that installs raiseload() on relationships not explicitly requested!
        for mjp in self.mjps:
            if mjp.query_object is None:
                # There's no Query object, but an MJP will still be formed; with options=None
                # Here, however, we're going to lazy load this relationship
                # Overview: https://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html
                # Decide how.

                if mjp.relationship.property.lazy in (None, True, 'select', 'immediate', 'selectin'):
                    # If `lazy` not configured -- override with the best option
                    # `selectin` is the most performant: a successor of `immediate`
                    rel_load = as_relation.selectinload(mjp.relationship)
                else:
                    # If `lazy` configured property -- just use the way it is configured
                    rel_load = as_relation.defaultload(mjp.relationship)
                    # NOTE: previously it was joinedload() by default.
                    # No unit-test were harmed by changing it to defaultload()

                # No query specified: do not load sub-relations
                rel_load.lazyload('*')
            else:
                # There is a query: can't lazyload, have to build the query
                # Here we use `contains_eager()` which indicates that the given relationship should
                # be eagerly loaded from columns stated manually in the query.
                # See: https://docs.sqlalchemy.org/en/latest/orm/loading_relationships.html#sqlalchemy.orm.contains_eager
                # In other words: we will build the query, make a join, and tell sqlalchemy that
                # the related model is present within the results of the query.
                # This logic is present in query.py
                mjp.options = [as_relation.contains_eager(mjp.relationship, alias=mjp.target_model_aliased)]

        # We can't do nothing else here, so we just return this MJP
        # TODO: remove ALL joining logic from MongoQuery and somehow move it here!!
        return self.mjps
