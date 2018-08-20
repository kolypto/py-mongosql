from sqlalchemy import sql, inspection

from sqlalchemy.orm.util import ORMAdapter
from sqlalchemy.sql import visitors
from sqlalchemy.sql.expression import and_


def _add_alias(join_clause, relationship, alias):
    right_mapper = relationship.prop.mapper
    adapter = ORMAdapter(
        alias,
        equivalents=right_mapper and
        right_mapper._equivalent_columns or {},
    ).replace

    def replace(elem):
        e = adapter(elem)
        if e is not None:
            return e

    join_clause = visitors.replacement_traverse(
        join_clause,
        {},
        replace)
    return join_clause


def outer_with_filter(query, alias, relation, filter_clause):
    left = relation.prop.parent
    left_info = inspection.inspect(left)
    right_info = inspection.inspect(alias)
    adapt_to = right_info.selectable
    adapt_from = left_info.selectable
    pj, sj, source, dest, \
        secondary, target_adapter = relation.prop._create_joins(
            source_selectable=adapt_from,
            dest_selectable=adapt_to,
            source_polymorphic=True,
            dest_polymorphic=True,
            of_type=right_info.mapper)
    if sj is not None:
        # note this is an inner join from secondary->right
        right = sql.join(secondary, alias, sj)
    else:
        right = alias
    onclause = and_(_add_alias(pj, relation, alias), filter_clause)
    return query.outerjoin(right, onclause)
