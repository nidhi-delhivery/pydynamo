from django.db.models.fields.related import ForeignKey

class Field(object):
    def to_python(self, val):
        pass

    def from_python(self, val):
        pass

    def clean(self, val):
        pass


class Integer(Field):
    def to_python(self, val):
        return int(val)


