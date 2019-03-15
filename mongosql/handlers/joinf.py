from __future__ import absolute_import

from .join import MongoJoin

class MongoFilteringJoin(MongoJoin):
    """ Joining relations: perform a real SQL JOIN to the related model, applying a filter to the
        whole result set (!)

        Note that this will distort the results of the original query:
        essentially, it will only return entities *having* at least one related entity with
        the given condition.

        This means that if you take an `Article`, make a 'joinf' to `Article.author`,
        and specify a filter with `age > 20`,
        you will get articles and their authors,
        but the articles *will be limited to only teenage authors*.
    """

    query_object_section_name = 'joinf'

    def _load_relationship_with_filter(self, query, as_relation, mjp):
        # This is the culprit: a joining method that does not do its job right... :)
        return self._load_relationship_with_filter__joinf(query, as_relation, mjp)
