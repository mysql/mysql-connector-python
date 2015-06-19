# MySQL Connector/Python - MySQL driver written in Python.


import django
from django.db.models.sql import compiler
from django.utils.six.moves import zip_longest


class SQLCompiler(compiler.SQLCompiler):
    def resolve_columns(self, row, fields=()):
        values = []
        index_extra_select = len(self.query.extra_select)
        bool_fields = ("BooleanField", "NullBooleanField")
        for value, field in zip_longest(row[index_extra_select:], fields):
            if (field and field.get_internal_type() in bool_fields and
                    value in (0, 1)):
                value = bool(value)
            values.append(value)
        return row[:index_extra_select] + tuple(values)

    if django.VERSION >= (1, 8):
        def as_subquery_condition(self, alias, columns, compiler):
            qn = compiler.quote_name_unless_alias
            qn2 = self.connection.ops.quote_name
            sql, params = self.as_sql()
            return '(%s) IN (%s)' % (', '.join('%s.%s' % (qn(alias), qn2(column)) for column in columns), sql), params
    else:
        def as_subquery_condition(self, alias, columns, qn):
            # Django 1.6
            qn2 = self.connection.ops.quote_name
            sql, params = self.as_sql()
            column_list = ', '.join(
                ['%s.%s' % (qn(alias), qn2(column)) for column in columns])
            return '({0}) IN ({1})'.format(column_list, sql), params


class SQLInsertCompiler(compiler.SQLInsertCompiler, SQLCompiler):
    pass


class SQLDeleteCompiler(compiler.SQLDeleteCompiler, SQLCompiler):
    pass


class SQLUpdateCompiler(compiler.SQLUpdateCompiler, SQLCompiler):
    pass


class SQLAggregateCompiler(compiler.SQLAggregateCompiler, SQLCompiler):
    pass

if django.VERSION < (1, 8):
    class SQLDateCompiler(compiler.SQLDateCompiler, SQLCompiler):
        pass

    if django.VERSION >= (1, 6):
        class SQLDateTimeCompiler(compiler.SQLDateTimeCompiler, SQLCompiler):
            # Django 1.6
            pass
