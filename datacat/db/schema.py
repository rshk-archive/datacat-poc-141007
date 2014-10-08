"""
Datacat - Database schema
"""

from collections import OrderedDict

from .utils import TableSchema


ALL_TABLES = OrderedDict()


def define_table(name, *a, **kw):
    table = TableSchema(name, *a, **kw)
    ALL_TABLES[name] = table
    return table


define_table('info', [
    ('key', 'CHARACTER VARYING (256) PRIMARY KEY'),
    ('value', 'TEXT'),
])

define_table('dataset', [
    ('id', 'SERIAL PRIMARY KEY'),
    ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('configuration', 'JSON'),
    ('source_ref', 'CHARACTER VARYING (128) UNIQUE'),
    ('resources', 'INTEGER[]'),
])

define_table('resource', [
    ('id', 'SERIAL PRIMARY KEY'),
    ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('configuration', 'JSON'),
    ('source_ref', 'CHARACTER VARYING (128) UNIQUE'),
])

define_table('resource_data', [
    ('id', 'SERIAL PRIMARY KEY'),
    ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
    ('metadata', 'JSON'),
    ('auto_metadata', 'JSON'),
    ('mimetype', 'CHARACTER VARYING (128)'),
    ('data_oid', 'INTEGER'),  # lobject oid
    ('hash', 'VARCHAR(128)'),  # ALGO:HASH
])

# define_table('data_source', [
#     ('id', 'CHARACTER VARCHAR (128) PRIMARY KEY'),
#     ('ctime', 'TIMESTAMP WITHOUT TIME ZONE'),
#     ('mtime', 'TIMESTAMP WITHOUT TIME ZONE'),
#     ('url', 'CHARACTER VARYING (2048)'),
#     ('connector', 'CHARACTER VARYING (256)'),
#     ('configuration', 'JSON'),
# ])
