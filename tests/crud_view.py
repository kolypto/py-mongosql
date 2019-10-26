from functools import wraps
from logging import getLogger

from mongosql import CrudViewMixin, StrictCrudHelper, StrictCrudHelperSettingsDict, saves_relations

from . import models
from flask import request, g, jsonify
from flask_jsontools import jsonapi, RestfulView

logger = getLogger(__name__)


def passthrough_decorator(f):
    """ A no-op decorator.
        It's only purpose is to see whether @saves_relations() works even when decorated with something else.
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        return f(*args, **kwargs)
    return wrapper


class RestfulModelView(RestfulView, CrudViewMixin):
    """ Base view class for all other views """
    crudhelper = None

    # RestfulView needs that for routing
    primary_key = None
    decorators = (jsonapi,)

    # Every response will have either { article: ... } or { articles: [...] }
    # Stick to the DRY principle: store the key name once
    entity_name = None
    entity_names = None

    # Implement the method that fetches the Query Object for this request
    def _get_query_object(self):
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
        """ Modify a returned instance """
        return self._mongoquery.pluck_instance(instance)

    def _save_hook(self, new: models.Article, prev: models.Article = None):
        # There's one special case for a failure: title='z'.
        # This is how unit-tests can test exceptions
        if new.title == 'z':
            # Simulate a bug
            raise RuntimeError(
                'This method inexplicably fails when title="z"'
            )
        super()._save_hook(new, prev)

    # region CRUD methods

    def list(self):
        """ List method: GET /article/ """
        # List results
        results = self._method_list()

        # Format response
        # NOTE: can't return map(), because it's not JSON serializable
        return {self.entity_names: results}

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
        item = self._method_get(id=id)
        return {self.entity_name: self._return_instance(item)}

    def create(self):
        # Trying to save many objects at once?
        if self.entity_names in request.get_json():
            return self.save_many()

        # Saving only one object
        input_entity_dict = request.get_json()[self.entity_name]
        instance = self._method_create(input_entity_dict)

        ssn = self._get_db_session()
        ssn.add(instance)
        ssn.commit()

        return {self.entity_name: self._return_instance(instance)}

    def save_many(self):
        # Get the input
        input_json = request.get_json()
        entity_dicts = input_json[self.entity_names]

        # Process
        results = self._method_create_or_update_many(entity_dicts)

        # Save
        ssn = self._get_db_session()
        ssn.add_all(res.instance for res in results if res.instance is not None)
        ssn.commit()

        # Log every error
        for res in results:
            if res.error:
                logger.exception(str(res.error), exc_info=res.error)

        # Results
        return {
            # Entities
            self.entity_names: [
                # Each one goes through self._return_instance()
                self._return_instance(res.instance) if res.instance else None
                for res in results
            ],
            # Errors
            'errors': {
                res.ordinal_number: str(res.error)
                for res in results
                if res.error
            },
        }

    def update(self, id):
        input_entity_dict = request.get_json()[self.entity_name]
        instance = self._method_update(input_entity_dict, id=id)

        ssn = self._get_db_session()
        ssn.add(instance)
        ssn.commit()

        return {self.entity_name: self._return_instance(instance)}

    def delete(self, id):
        instance = self._method_delete(id=id)

        ssn = self._get_db_session()
        ssn.delete(instance)
        ssn.commit()

        return {self.entity_name: self._return_instance(instance)}

    # endregion


class ArticleView(RestfulModelView):
    """ Full-featured CRUD view """

    # First, configure a CrudHelper
    crudhelper = StrictCrudHelper(
        # The model to work with
        models.Article,
        **StrictCrudHelperSettingsDict(
            # Read-only fields, as a callable (just because)
            ro_fields=lambda: ('id', 'uid',),
            legacy_fields=('removed_column',),
            # MongoQuery settings
            aggregate_columns=('id', 'data',),  # have to explicitly enable aggregation for columns
            query_defaults=dict(
                sort=('id-',),
            ),
            writable_properties=True,
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

    primary_key = ('id',)
    decorators = (jsonapi,)

    entity_name = 'article'
    entity_names = 'articles'

    def _method_create(self, entity_dict: dict) -> object:
        instance = super()._method_create(entity_dict)
        instance.uid = 3  # Manually set ro field value, because the client can't
        return instance

    # Our completely custom stuff

    @passthrough_decorator  # no-op to demonstrate that it still works
    @saves_relations('comments')
    def save_comments(self, new, prev=None, comments=None):
        # Just store it in the class for unit-test to find it
        self.__class__._save_comments__args = dict(new=new, prev=prev, comments=comments)

    @passthrough_decorator  # no-op to demonstrate that it still works
    @saves_relations('user', 'comments')
    def save_relations(self, new, prev=None, user=None, comments=None):
        # Just store it in the class for unit-test to find it
        self.__class__._save_relations__args = dict(new=new, prev=prev, user=user, comments=comments)

    @saves_relations('removed_column')
    def save_removed_column(self, new, prev=None, removed_column=None):
        # Store
        self.__class__._save_removed_column = dict(removed_column=removed_column)

    _save_comments__args = None
    _save_relations__args = None
    _save_removed_column = None


class GirlWatcherView(RestfulModelView):
    crudhelper = StrictCrudHelper(
        models.GirlWatcher,
        **StrictCrudHelperSettingsDict(
            # Read-only fields, as a callable (just because)
            ro_fields=('id', 'favorite_id',),
            allowed_relations=('good', 'best')
        )
    )

    primary_key = ('id',)
    decorators = (jsonapi,)

    entity_name = 'girlwatcher'
    entity_names = 'girlwatchers'

    def _return_instance(self, instance):
        instance = super()._return_instance(instance)

        # TypeError: Object of type _AssociationList is not JSON serializable
        for k in ('good_names', 'best_names'):
            if k in instance:
                # Convert this _AssociationList() object into a real list
                instance[k] = list(instance[k])

        return instance

