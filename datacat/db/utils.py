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
