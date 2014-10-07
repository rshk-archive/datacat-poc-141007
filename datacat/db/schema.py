"""
Datacat - Database schema
"""

from collections import OrderedDict


class TableSchema(object):
    def __init__(self, name, fields=None, primary_key=None):
        self.name = name
        if fields is None:
            fields = []
        self.fields = fields
        self.primary_key = primary_key

    def get_create_sql(self):
        table_definition = [
            self._build_field_def(x) for x in self.fields]

        if self.primary_key is not None:
            table_definition.append(
                'PRIMARY KEY ({0})'
                .format(', '.join(
                    '"{0}"'.format(x) for x in self.primary_key)))

        return 'CREATE TABLE "{name}" ({definition});'.format(
            name=self.name, definition=", ".join(table_definition))

    def get_drop_sql(self):
        return 'DROP TABLE "{name}";'.format(name=self.name)

    def _build_field_def(self, field_def):
        name, definition = field_def
        return '"{0}" {1}'.format(name, definition)


ALL_TABLES = OrderedDict()


def define_table(name, *a, **kw):
    table = TableSchema(name, *a, **kw)
    ALL_TABLES[name] = table
    return table


define_table('info', [
    ('key', 'CHARACTER VARYING (256) PRIMARY KEY')
    ('value', 'TEXT')
])

define_table('dataset', [
    ('id', 'SERIAL PRIMARY KEY'),
    ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('configuration', 'JSON'),
])

define_table('resource', [
    ('id', 'SERIAL PRIMARY KEY'),
    ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('configuration', 'JSON'),
])

define_table('dataset_resource', [
    ('dataset_id', 'INTEGER REFERENCES dataset (id)'),
    ('resource_id', 'INTEGER REFERENCES resource (id)'),
    ('order', 'INTEGER DEFAULT 0'),
], primary_key=('dataset_id', 'resource_id'))

# define_table('data_source', [
#     ('id', 'CHARACTER VARCHAR (128) PRIMARY KEY'),
#     ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
#     ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
#     ('url', 'CHARACTER VARYING (2048)'),
#     ('connector', 'CHARACTER VARYING (256)'),
#     ('configuration', 'JSON'),
# ])
