from mongosql import CrudViewMixin, StrictCrudHelper

from . import models
from flask import request, g
from flask.ext.jsontools import jsonapi, RestfulView


class ArticlesView(RestfulView, CrudViewMixin):
    """ Full-featured CRUD view """

    # We're strict, yeah
    crudhelper = StrictCrudHelper(models.Article,
        ro_fields=('id', 'uid',),
        allow_relations=('user', 'user.comments'),
        query_defaults={
            'sort': ['id-'],
        },
        maxitems=2,
    )

    # RestfulView needs that for routing
    primary_key = ('id',)
    decorators = (jsonapi,)

    # DRY: store it once
    entity_name = 'article'

    @staticmethod
    def _getQueryObject():
        """ Get Query Object from request

        :rtype: dict | None
        """
        return (request.get_json() or {}).get('query', None)

    @staticmethod
    def _getDbSession():
        """ Get database Session

        :rtype: sqlalchemy.orm.Session
        """
        return g.db

    def _query(self):
        return self._getDbSession().query(self.crudhelper.model)

    #region Collection methods

    def list(self):
        return { self.entity_name+'s': self._method_list(self._getQueryObject()) }

    def create(self):
        entity = self._method_create(request.get_json()[self.entity_name])
        entity.uid = 3  # Manually set ro field value

        ssn = self._getDbSession()
        ssn.add(entity)
        ssn.commit()

        return {self.entity_name: entity}

    #endregion

    #region Single entity methods

    def get(self, id):
        return { self.entity_name: self._method_get(self._getQueryObject(), id=id) }

    def replace(self, id):
        entity, prev_entity = self._method_replace(request.get_json()[self.entity_name], id=id)

        ssn = self._getDbSession()
        ssn.expunge(prev_entity)  # Remove so it does not cause 'conflicts with persistent instance' errors
        ssn.merge(entity)
        ssn.commit()

        return {self.entity_name: entity}

    def update(self, id):
        entity = self._method_update(request.get_json()[self.entity_name], id=id)

        ssn = self._getDbSession()
        ssn.add(entity)
        ssn.commit()

        return {self.entity_name: entity}

    def delete(self, id):
        entity = self._method_delete(id=id)

        ssn = self._getDbSession()
        ssn.delete(entity)
        ssn.commit()

        return {self.entity_name: entity}

    #endregion
