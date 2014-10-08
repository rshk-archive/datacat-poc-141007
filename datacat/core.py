"""
Core functionality for datacat.
Mostly wrappers around database queries, etc.
"""

import hashlib
import json
from datetime import datetime

from flask import g, current_app
from flask.config import Config
from werkzeug import LocalProxy
from werkzeug.exceptions import NotFound

from datacat.db import querybuilder, connect, create_tables, drop_tables
from datacat.utils.files import file_read_chunks


class DatacatCore(object):
    def __init__(self, config=None):
        self.config = Config()
        if config is not None:
            self.config.update(config)

    @property
    def db(self):
        if getattr(self, '_db', None) is None:
            self._db = connect(**self.config['DATABASE'])
            self._db.autocommit = False
        return self._db

    @property
    def admin_db(self):
        if getattr(self, '_admin_db', None) is None:
            self._admin_db = connect(**self.config['DATABASE'])
            self._admin_db.autocommit = True
        return self._admin_db

    def create_tables(self):
        create_tables(self.admin_db)

    def drop_tables(self):
        drop_tables(self.admin_db)

    # ------------------------------------------------------------
    # Resource data CRUD
    # ------------------------------------------------------------

    def resource_data_iter(self):
        """Iterate over resource attributes"""

        with self.db, self.db.cursor() as cur:
            cur.execute("SELECT * FROM resource_data")
            for row in cur.fetchall():
                yield row

    def resource_data_create(self, stream, metadata=None, mimetype=None):
        """Create resource data from a stream"""

        resource_hash = hashlib.sha1()
        with self.db, self.db.cursor() as cur:
            lobj = self.db.lobject(oid=0, mode='wb')
            oid = lobj.oid
            for chunk in file_read_chunks(stream):
                lobj.write(chunk)
                resource_hash.update(chunk)
            lobj.close()

            data = {
                'ctime': datetime.now(),
                'mtime': datetime.now(),
                'metadata': json.dumps(metadata),
                'mimetype': mimetype or 'application/octet-stream',
                'data_oid': oid,
                'hash': 'sha1:{0}'.format(resource_hash.hexdigest()),
            }

            query = querybuilder.insert('resource_data', data)
            cur.execute(query, data)
            resource_data_id = cur.fetchone()[0]

        return resource_data_id

    def resource_data_get_info(self, objid):
        query = querybuilder.select_pk('resource_data')
        with self.db, self.db.cursor() as cur:
            cur.execute(query, {'id': objid})
            return cur.fetchone()

    def resource_data_read(self, objid):
        resource = self.resource_data_get_info(objid)
        with self.db:
            lobj = self.db.lobject(oid=resource['data_oid'], mode='rb')
            return lobj.read()

    def resource_data_copy(self, objid, dest):
        resource = self.resource_data_get_info(objid)
        with self.db:
            lobj = self.db.lobject(oid=resource['data_oid'], mode='rb')
            for chunk in file_read_chunks(lobj):
                dest.write(chunk)

    def resource_data_update(self, objid, stream=None, metadata=None,
                             mimetype=None):

        # Get the original object, to check for its existence
        # and to get the oid of the lobject holding the data.
        original = self.resource_data_get_info(objid)

        data = {
            'id': objid,
            'mtime': datetime.now(),
        }

        if metadata is not None:
            data['metadata'] = json.dumps(metadata)
        if mimetype is not None:
            data['mimetype'] = mimetype

        with self.db, self.db.cursor() as cur:
            if stream is not None:
                # Update the lobject with data from the stream
                resource_hash = hashlib.sha1()
                lobj = self.db.lobject(oid=original['data_oid'], mode='wb')
                for chunk in file_read_chunks(stream):
                    lobj.write(chunk)
                    resource_hash.update(chunk)
                lobj.close()
                data['hash'] = 'sha1:{0}'.format(resource_hash.hexdigest())

            query = querybuilder.update('resource_data', data)
            cur.execute(query, data)

    def resource_data_remove(self, objid):
        # We need the OID to remove the lobject
        original = self.resource_data_get_info(objid)

        with self.db, self.db.cursor() as cur:
            lobject = db.lobject(oid=resource['data_oid'], mode='rb')
            data = lobject.read()
            lobject.close()

            cur.execute(querybuilder.delete('resource_data'), {'id': objid})

    # ------------------------------------------------------------
    # Dataset / resource CRUD
    # ------------------------------------------------------------

    def create_resource(self, resource):
        return self._dsres_create('resource', resource)

    def update_resource(self, resource_id, resource):
        return self._dsres_update('resource', resource_id, resource)

    def get_resource(self, resource_id):
        return self._dsres_get('resource', resource_id)

    def list_resources(self, offset=None, limit=None):
        return self._dsres_list('resource', offset=offset, limit=limit)

    def delete_resource(self, resource_id):
        return self._dsres_delete('resource', resource_id)

    def create_dataset(self, dataset):
        return self._dsres_create('dataset', dataset)

    def update_dataset(self, dataset_id, dataset):
        return self._dsres_update('dataset', dataset_id, dataset)

    def get_dataset(self, dataset_id):
        return self._dsres_get('dataset', dataset_id)

    def list_datasets(self, offset=None, limit=None):
        return self._dsres_list('dataset', offset=offset, limit=limit)

    def delete_dataset(self, dataset_id):
        return self._dsres_delete('dataset', dataset_id)

    def add_dataset_resource(self, dataset_id, resource_id, order=0):
        data = {
            'dataset_id': dataset_id,
            'resource_id': resource_id,
            'order': order}
        query = querybuilder.insert(
            'dataset_resource', data=data, table_key=None)
        with self.db, self.db.cursor() as cur:
            cur.execute(query, data)

    def delete_dataset_resource(self, dataset_id, resource_id):
        data = {'dataset_id': dataset_id, 'resource_id': resource_id}
        query = ("DELETE FROM dataset_resource"
                 " WHERE dataset_id=%(dataset_id)s"
                 " AND resource_id=%(resource_id)s")
        with self.db, self.db.cursor() as cur:
            cur.execute(query, data)

    def move_dataset_resource(self, dataset_id, resource_id, order):
        data = {'dataset_id': dataset_id,
                'resource_id': resource_id,
                'order': order}
        query = ("UPDATE dataset_resource"
                 " SET order=%(order)s"
                 " WHERE dataset_id=%(dataset_id)s"
                 " AND resource_id=%(resource_id)s")
        with self.db, self.db.cursor() as cur:
            cur.execute(query, data)

    # ------------------------------------------------------------
    # Rows and datasets have the same schema:
    # let's use some common functions for them..
    # ------------------------------------------------------------

    def _dsres_create(self, name, obj):
        data = {
            'configuration': json.dumps(obj),
            'ctime': datetime.now(),
            'mtime': datetime.now(),
        }
        query = querybuilder.insert(name, data)
        with self.db, self.db.cursor() as cur:
            cur.execute(query, data)
            return cur.fetchone()[0]

    def _dsres_update(self, name, obj_id, obj):
        data = {
            'id': obj_id,
            'configuration': json.dumps(obj),
            'mtime': datetime.now(),
        }
        query = querybuilder.update(name, data)
        with self.db, self.db.cursor() as cur:
            cur.execute(query, data)

    def _dsres_get(self, name, obj_id):
        query = querybuilder.select_pk(name)
        with self.db, self.db.cursor() as cur:
            cur.execute(query, dict(id=obj_id))
            row = cur.fetchone()

        if row is None:
            raise NotFound()

        return self._row_to_obj(row)

    def _dsres_list(self, name, offset=None, limit=None):
        query = querybuilder.select_paged(
            name, offset=offset, limit=limit)
        with self.db, self.db.cursor() as cur:
            cur.execute(query)
            for row in cur.fetchall():
                yield self._row_to_obj(row)

    def _dsres_from_row(self, row):
        obj = row['configuration']
        obj['_id'] = row['id']
        obj['_ctime'] = row['ctime']
        obj['_mtime'] = row['mtime']
        return obj

    def _dsres_delete(self, name, obj_id):
        query = querybuilder.delete(name)
        with self.db, self.db.cursor() as cur:
            cur.execute(query, dict(id=obj_id))


def get_current_datacat():
    """Get the "current" instance of the datacat app"""

    datacat = getattr(g, '_datacat', None)
    if datacat is None:
        datacat = g._datacat = DatacatCore(current_app.config)
    return datacat


datacat_core = LocalProxy(get_current_datacat)
