from mongosql import CrudViewMixin, StrictCrudHelper
import collections

from . import models
from flask import request, g
from flask_jsontools import jsonapi, RestfulView


def project(item, projection):
    result = {}
    if isinstance(item, collections.Iterable):
        return [project(i, projection) for i in item]
    for key, value in projection.items():
        if isinstance(value, dict):
            result[key] = project(getattr(item, key, None), value)
        if value == 1:
            result[key] = getattr(item, key, None)
    return result


class ArticlesView(RestfulView, CrudViewMixin):
    """ Full-featured CRUD view """

    # We're strict, yeah
    crudhelper = StrictCrudHelper(models.Article,
        ro_fields=('id', 'uid',),
        allow_relations=('user', 'user.comments', 'comments'),
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

    @property
    def _qo(self):
        """ Get Query Object from request

        :rtype: dict | None
        """
        return (request.get_json() or {}).get('query', None)

    def _db(self):
        """ Get database Session

        :rtype: sqlalchemy.orm.Session
        """
        return g.db

    def _query(self):  # Implemented
        return self._db().query(self.crudhelper.model)

    #region Collection methods

    def list(self):
        data, projection = self._method_list(self._qo)
        if projection:
            data = project(data, projection)
        return { self.entity_name+'s':  data}

    def create(self):
        instance = self._method_create(request.get_json()[self.entity_name])
        instance.uid = 3  # Manually set ro field value

        ssn = self._db()
        ssn.add(instance)
        ssn.commit()

        return {self.entity_name: instance}

    #endregion

    #region Single entity methods

    def get(self, id):
        item, projection = self._method_get(self._qo, id=id)
        if projection:
            item = project(item, projection)
        return { self.entity_name:  item }

    def update(self, id):
        instance = self._method_update(request.get_json()[self.entity_name], id=id)

        ssn = self._db()
        ssn.add(instance)
        ssn.commit()

        return {self.entity_name: instance}

    def delete(self, id):
        instance = self._method_delete(id=id)

        ssn = self._db()
        ssn.delete(instance)
        ssn.commit()

        return {self.entity_name: instance}

    #endregion
