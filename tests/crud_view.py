from mongosql import CrudViewMixin, StrictCrudHelper, StrictCrudHelperSettingsDict

from . import models
from flask import request, g
from flask_jsontools import jsonapi, RestfulView


class ArticlesView(RestfulView, CrudViewMixin):
    """ Full-featured CRUD view """

    # First, configure a CrudHelper
    crudhelper = StrictCrudHelper(
        # The model to work with
        models.Article,
        **StrictCrudHelperSettingsDict(
            # Read-only fields, as a callable (just because)
            ro_fields=lambda: ('id', 'uid',),
            # MongoQuery settings
            aggregate_columns=('id', 'data',),  # have to explicitly enable aggregation for columns
            query_defaults=dict(
                sort=('id-',),
            ),
            max_items=2,
            # Related entities configuration
            allowed_relations=('user', 'comments'),
            related={
                'user': dict(
                    # Exclude @property by default
                    default_exclude=('user_calculated',),
                    allowed_relations=('comments',),
                    related={
                        'comments': dict(
                            # Exclude @property by default
                            default_exclude=('comment_calc',),
                            # No further joins
                            join_enabled=False,
                        )
                    }
                ),
                'comments': dict(
                    # Exclude @property by default
                    default_exclude=('comment_calc',),
                    # No further joins
                    join_enabled=False,
                ),
            },
        )
    )

    # ensure_loaded: always load these columns and relationships
    # This is necessary in case some custom code relies on it
    ensure_loaded = ('data', 'comments')  # that's a weird requirement, but since the user is supposed to use projections, it will be excluded

    # RestfulView needs that for routing
    primary_key = ('id',)
    decorators = (jsonapi,)

    # Every response will have either { article: ... } or { articles: [...] }
    # Stick to the DRY principle: store the key name once
    entity_name = 'article'
    entity_names = 'articles'

    # This is our helper method that extracts the QueryObject from the request
    # No interface requires this ; this is purely for our pleasure
    @property
    def _query_object(self):
        """ Get Query Object from request

        :rtype: dict | None
        """
        return (request.get_json() or {}).get('query', None)

    # CrudViewMixin demands: needs to be able to get a session so that it can run a query
    def _get_db_session(self):
        """ Get database Session

        :rtype: sqlalchemy.orm.Session
        """
        return g.db

    # This is our method: it plucks an instance using the current projection
    # This is just convenience: if the user has requested
    def _return_instance(self, instance):
        """ Modify a returned instance: for GET and LIST methods

            Note: create(), update(), delete() instances are returned unadulterated.
        """
        return self._mongoquery.pluck_instance(instance)

    # region CRUD methods

    def list(self):
        """ List method: GET /article/ """
        # List results
        results = self._method_list(self._query_object)

        # Format response
        # NOTE: can't return map(), because it's not JSON serializable
        return {self.entity_name+'s': results}

    def _method_list_result__groups(self, dicts):
        """ Format the result from GET /article/ when the result is a list of dicts (GROUP BY) """
        return list(dicts)  # our JSON serializer does not like generators. Have to make it into a list

    def _method_list_result__entities(self, entities):
        """ Format the result from GET /article/ when the result is a list of sqlalchemy entities """
        # Pluck results: apply projection to the result set
        # This is just our good manners: if the client has requested certain fields, we return only those they requested.
        # Even if our code loads some more columns (and it does!), the client will always get what they requested.
        return list(map(self._return_instance, entities))

    def get(self, id):
        item = self._method_get(self._query_object, id=id)
        return {self.entity_name: self._return_instance(item)}

    def create(self):
        input_entity_dict = request.get_json()[self.entity_name]
        instance = self._method_create(input_entity_dict)
        instance.uid = 3  # Manually set ro field value, because the client can't

        ssn = self._get_db_session()
        ssn.add(instance)
        ssn.commit()

        return {self.entity_name: instance}

    def update(self, id):
        input_entity_dict = request.get_json()[self.entity_name]
        instance = self._method_update(input_entity_dict, id=id)

        ssn = self._get_db_session()
        ssn.add(instance)
        ssn.commit()

        return {self.entity_name: instance}

    def delete(self, id):
        instance = self._method_delete(id=id)

        ssn = self._get_db_session()
        ssn.delete(instance)
        ssn.commit()

        return {self.entity_name: instance}

    # endregion
