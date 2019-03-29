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

    def _choose_relationship_loading_strategy(self, mjp):
        if mjp.has_nested_query:
            # Quite intentionally, we will use a regular JOIN here.
            # It will remove rows that 1) have no related rows, and 2) do not match our filter conditions.
            # This is what the user wants when they use 'joinf' handler.
            return self.RELSTRATEGY_JOINF
        else:
            return self.RELSTRATEGY_EAGERLOAD

    # merge() is not implemented for joinf, because the results wouldn't be compatible
    merge = NotImplemented
