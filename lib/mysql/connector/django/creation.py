# MySQL Connector/Python - MySQL driver written in Python.

import django
from django.db import models
from django.db.backends.creation import BaseDatabaseCreation

if django.VERSION < (1, 7):
    from django.db.backends.util import truncate_name
else:
    from django.db.backends.utils import truncate_name

class DatabaseCreation(BaseDatabaseCreation):
    """Maps Django Field object with MySQL data types
    """
    def __init__(self, connection):
        super(DatabaseCreation, self).__init__(connection)

        self.data_types = {
            'AutoField': 'integer AUTO_INCREMENT',
            'BinaryField': 'longblob',
            'BooleanField': 'bool',
            'CharField': 'varchar(%(max_length)s)',
            'CommaSeparatedIntegerField': 'varchar(%(max_length)s)',
            'DateField': 'date',
            'DateTimeField': 'datetime',  # ms support set later
            'DecimalField': 'numeric(%(max_digits)s, %(decimal_places)s)',
            'FileField': 'varchar(%(max_length)s)',
            'FilePathField': 'varchar(%(max_length)s)',
            'FloatField': 'double precision',
            'IntegerField': 'integer',
            'BigIntegerField': 'bigint',
            'IPAddressField': 'char(15)',

            'GenericIPAddressField': 'char(39)',
            'NullBooleanField': 'bool',
            'OneToOneField': 'integer',
            'PositiveIntegerField': 'integer UNSIGNED',
            'PositiveSmallIntegerField': 'smallint UNSIGNED',
            'SlugField': 'varchar(%(max_length)s)',
            'SmallIntegerField': 'smallint',
            'TextField': 'longtext',
            'TimeField': 'time',  # ms support set later
        }

        # Support for microseconds
        if self.connection.mysql_version >= (5, 6, 4):
            self.data_types.update({
                'DateTimeField': 'datetime(6)',
                'TimeField': 'time(6)',
            })

    def sql_table_creation_suffix(self):
        suffix = []
        if django.VERSION < (1, 7):
            if self.connection.settings_dict['TEST_CHARSET']:
                suffix.append('CHARACTER SET {0}'.format(
                    self.connection.settings_dict['TEST_CHARSET']))
            if self.connection.settings_dict['TEST_COLLATION']:
                suffix.append('COLLATE {0}'.format(
                    self.connection.settings_dict['TEST_COLLATION']))

        else:
            test_settings = self.connection.settings_dict['TEST']
            if test_settings['CHARSET']:
                suffix.append('CHARACTER SET %s' % test_settings['CHARSET'])
            if test_settings['COLLATION']:
                suffix.append('COLLATE %s' % test_settings['COLLATION'])

        return ' '.join(suffix)

    if django.VERSION < (1, 6):
        def sql_for_inline_foreign_key_references(self, field, known_models,
                                                  style):
            "All inline references are pending under MySQL"
            return [], True
    else:
        def sql_for_inline_foreign_key_references(self, model, field,
                                                  known_models, style):
            "All inline references are pending under MySQL"
            return [], True

    def sql_for_inline_many_to_many_references(self, model, field, style):
        opts = model._meta
        qn = self.connection.ops.quote_name

        columndef = '    {column} {type} {options},'
        table_output = [
            columndef.format(
                column=style.SQL_FIELD(qn(field.m2m_column_name())),
                type=style.SQL_COLTYPE(models.ForeignKey(model).db_type(
                    connection=self.connection)),
                options=style.SQL_KEYWORD('NOT NULL')
            ),
            columndef.format(
                column=style.SQL_FIELD(qn(field.m2m_reverse_name())),
                type=style.SQL_COLTYPE(models.ForeignKey(field.rel.to).db_type(
                    connection=self.connection)),
                options=style.SQL_KEYWORD('NOT NULL')
            ),
        ]

        deferred = [
            (field.m2m_db_table(), field.m2m_column_name(), opts.db_table,
                opts.pk.column),
            (field.m2m_db_table(), field.m2m_reverse_name(),
                field.rel.to._meta.db_table, field.rel.to._meta.pk.column)
        ]
        return table_output, deferred

    def sql_destroy_indexes_for_fields(self, model, fields, style):
        # Django 1.6
        if len(fields) == 1 and fields[0].db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(
                fields[0].db_tablespace)
        elif model._meta.db_tablespace:
            tablespace_sql = self.connection.ops.tablespace_sql(
                model._meta.db_tablespace)
        else:
            tablespace_sql = ""
        if tablespace_sql:
            tablespace_sql = " " + tablespace_sql

        field_names = []
        qn = self.connection.ops.quote_name
        for f in fields:
            field_names.append(style.SQL_FIELD(qn(f.column)))

        index_name = "{0}_{1}".format(model._meta.db_table,
                                      self._digest([f.name for f in fields]))

        return [
            style.SQL_KEYWORD("DROP INDEX") + " " +
            style.SQL_TABLE(qn(truncate_name(index_name,
                self.connection.ops.max_name_length()))) + " " +
            style.SQL_KEYWORD("ON") + " " +
            style.SQL_TABLE(qn(model._meta.db_table)) + ";",
        ]
