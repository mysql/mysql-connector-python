# MySQL Connector/Python - MySQL driver written in Python.

import django
from django.db.backends import BaseDatabaseValidation

if django.VERSION < (1, 7):
    from django.db import models
else:
    from django.core import checks
    from django.db import connection


class DatabaseValidation(BaseDatabaseValidation):
    if django.VERSION < (1, 7):
        def validate_field(self, errors, opts, f):
            """
            MySQL has the following field length restriction:
            No character (varchar) fields can have a length exceeding 255
            characters if they have a unique index on them.
            """
            varchar_fields = (models.CharField,
                              models.CommaSeparatedIntegerField,
                              models.SlugField)
            if isinstance(f, varchar_fields) and f.max_length > 255 and f.unique:
                msg = ('"%(name)s": %(cls)s cannot have a "max_length" greater '
                       'than 255 when using "unique=True".')
                errors.add(opts, msg % {'name': f.name,
                                        'cls': f.__class__.__name__})

    else:
        def check_field(self, field, **kwargs):
            """
            MySQL has the following field length restriction:
            No character (varchar) fields can have a length exceeding 255
            characters if they have a unique index on them.
            """
            # Django 1.7
            errors = super(DatabaseValidation, self).check_field(field,
                                                                 **kwargs)

            # Ignore any related fields.
            if getattr(field, 'rel', None) is None:
                field_type = field.db_type(connection)

                if (field_type.startswith('varchar')  # Look for CharFields...
                        and field.unique  # ... that are unique
                        and (field.max_length is None or
                                     int(field.max_length) > 255)):
                    errors.append(
                        checks.Error(
                            ('MySQL does not allow unique CharFields to have a '
                             'max_length > 255.'),
                            hint=None,
                            obj=field,
                            id='mysql.E001',
                        )
                )
            return errors
