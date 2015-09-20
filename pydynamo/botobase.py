import sys
import uuid
import boto.dynamodb
import boto.dynamodb.layer2

from datetime import datetime

from django.db.models import Field
from django.forms.fields import ValidationError
from django.utils import six
from django.db.models.fields import DateTimeField

from boto.dynamodb.exceptions import (
    DynamoDBResponseError, DynamoDBKeyNotFoundError)

from . import DYNAMO_SECRET_KEY, DYNAMO_ACCESS_KEY

from boto.dynamodb2.exceptions import ProvisionedThroughputExceededException

from pydynamo.exception import (InvalidConfiguration, DataNotFound)
from pydynamo.app_settings import DEFAULT_NAMES, READ_UNITS, WRITE_UNITS


class DynamoBase(object):
    """
    Description:
        Basically this class is a manager class for django model class
    """
    ACCESS_KEY_ID = DYNAMO_ACCESS_KEY
    SECRET_ACCESS_KEY = DYNAMO_SECRET_KEY

    READ_UNIT = READ_UNITS
    WRITE_UNIT = WRITE_UNITS

    def __init__(self, cls, model_name):
        self.table_name = model_name
        self.model = cls
        self.boto_item = None

    @property
    def create_table(self):
        """
        Description:
            this manager creates a table in dynamodb if table does not exist with same name
        Returns:
            table: created table instance
        """
        connection = self.make_connection()
        table_schema = connection.create_schema(
            hash_key_name=self.model._meta.hash_key_name,
            hash_key_proto_value=str)
        try:
            table = connection.get_table(self.table_name)
        except DynamoDBResponseError:
            table = connection.create_table(
                name=self.table_name,
                schema=table_schema,
                read_units=self.model._meta.read_units or self.READ_UNIT,
                write_units=self.model._meta.write_units or self.WRITE_UNIT)
        return table

    def make_connection(self):
        """
        Description:
            create connection with dynamodb
        Raises:
            InvalidConfiguration : if not paasing hash key which is mandatory for creating a connection
        Retruns:
            connection: connection object
        """
        if not self.model._meta.hash_key_name:
            raise InvalidConfiguration('Hash Key is not configured')
        connection = boto.dynamodb.connect_to_region(
            settings.REGION,
            aws_access_key_id=self.ACCESS_KEY_ID,
            aws_secret_access_key=self.SECRET_ACCESS_KEY)
        return connection

    def __validate_data(self, data):
        """
        Description:
            Validates data which we are using for creating item that is valid or not
        Args:
            data: json data {"amount":1, "tax":"2"}
        """
        obj = self.model(**data)
        has_key = obj.get_hash_key()
        obj.clean_fields()
        for k, v in obj.__dict__.items():
            if k not in data:
                if isinstance(v, datetime):
                    v = v.strftime("%Y-%m-%d")
                data[k] = v
        data['hash_key'] = has_key
        return True

    def create_item(self, data, batch=False):
        """
        Description:
            creates item in dynaodb with cleaned data
        Args:
            data: json data {"amount":1, "tax":"2"}
        Returns:
            list of created data
        """
        item_list = []
        if not isinstance(data, list):
            data = [data]

        # Validate data according to fields configure
        for _d in data:
            self.__class__.__validate_data(self, _d)
        table = self.create_table
        # save data to db after validating all fields
        for _d in data:
            hash_key = _d.pop('hash_key')
            item = table.new_item(
                hash_key=hash_key, attrs=_d)
            if not batch:
                item.put()
            item_list.append(item)
        return item_list

    def get_item(self, key_value, attributes_to_get=None):
        """
        Description:
            retrieve item from dynamodb for a specific hash_key
        Args:
            key_value: value of hash_key for which item has to be retrieved
            attributes_to_get: takes list of fetching attribute, if we d'nt
                                provide this, fetch all attribute with this
                                hash key
        Raises:
            DataNotFound: if passed hash key is not found in dynamodb
        Returns:
            model_obj: obj of model for which get operation is hitting
        """
        table = self.create_table
        try:
            item_data = table.get_item(hash_key=key_value, attributes_to_get=attributes_to_get)
        except DynamoDBKeyNotFoundError:
            raise DataNotFound('Hash Key is invalid')
        self.boto_item = item_data
        fields = self.model._meta.fields
        dict_data = {}
        for _f in fields:
            dict_data[_f] = item_data.get(_f)
        model_obj = self.model(**dict_data)
        setattr(
            model_obj, self.model._meta.hash_key_name,
            item_data[self.model._meta.hash_key_name])
        return model_obj

    def update_item(self, attribute_updates=None):
        """
        Description:
            Update value in database
        Args:
            attribute_updates: dict of attributes to be updated
        """
        if self.boto_item:
            for each in attribute_updates:
                self.boto_item[each] = attribute_updates[each]
            self.boto_item.put()

    def delete_item(self, key_value):
        """
        Description:
            Delete specific item for a particular id(hash_key)
        """
        table = self.create_table
        item = table.get_item(hash_key=key_value)
        item.delete()

    def bulk_insert(self, items_list):
        """
        Description:
            insert data in dynamodb in a batch
        Args:
            list of dictionary data to be inserted
        """
        conn = self.make_connection()
        table = self.create_table
        batch_list = conn.new_batch_write_list()
        items_list = self.create_item(items_list, batch=True)
        batch_list.add_batch(table, puts=items_list)
        try:
            conn.batch_write_item(batch_list)
        except ProvisionedThroughputExceededException, e:
            return e.message


class Options(object):
    """
    Description:
        class for enabling options like ordering, hash_keyname using Meta class.
        also generates meta data for selected table
    """
    def __init__(self, meta, cls, app_label=None, **kwargs):
        self.model_name = None
        self.ordering = []
        self.index_fields = []
        self.permissions = []
        self.object_name, self.app_label = None, app_label
        self.meta = meta
        self.pk = None
        self.model = cls
        self.construct_meta_attrs(cls)

    def construct_meta_attrs(self, cls):
        """
        Descirption:
            construct meta attribute from META class
        """
        cls._meta = self
        self.model = cls

        self.object_name = cls.__name__
        self.model_name = self.object_name.lower()

        if self.meta:
            meta_attrs = self.meta.__dict__.copy()
            for name in self.meta.__dict__:
                # Ignore any private attributes that Django doesn't care about.
                # NOTE: We can't modify a dictionary's contents while looping
                # over it, so we loop over the *original* dictionary instead.
                if name.startswith('_'):
                    del meta_attrs[name]
            for attr_name in DEFAULT_NAMES:
                if attr_name in meta_attrs:
                    setattr(self, attr_name, meta_attrs.pop(attr_name))
                elif hasattr(self.meta, attr_name):
                    setattr(self, attr_name, getattr(self.meta, attr_name))

    @property
    def fields(self):
        """
        Description:
            fetch all fields of a model
        """
        fields = dict()
        for _attr in self.model.__dict__.copy():
            if isinstance(self.model.__dict__[_attr], Field):
                fields[_attr] = self.model.__dict__[_attr]
        return fields


class ModelBase(type):
    """
    Metaclass for all models.
    """
    def __new__(cls, name, bases, attrs):
        super_new = super(ModelBase, cls).__new__

        # six.with_metaclass() inserts an extra class called 'NewBase' in the
        # inheritance tree: Model -> NewBase -> object. But the initialization
        # should be executed only once for a given model class.

        # attrs will never be empty for classes declared in the standard way
        # (ie. with the `class` keyword). This is quite robust.
        if name == 'NewBase' and attrs == {}:
            return super_new(cls, name, bases, attrs)

        # Also ensure initialization is only performed for subclasses of Model
        # (excluding Model class itself).
        parents = [b for b in bases if isinstance(b, ModelBase) and
                not (b.__name__ == 'NewBase' and b.__mro__ == (b, object))]
        if not parents:
            return super_new(cls, name, bases, attrs)

        # Create the class.
        module = attrs.pop('__module__')
        new_class = super_new(cls, name, bases, {'__module__': module})
        attr_meta = attrs.pop('Meta', None)
        if not attr_meta:
            meta = getattr(new_class, 'Meta', None)
        else:
            meta = attr_meta

        kwargs = {}
        if getattr(meta, 'app_label', None) is None:
            # Figure out the app_label by looking one level up.
            # For 'django.contrib.sites.models', this would be 'sites'.
            model_module = sys.modules[new_class.__module__]
            kwargs = {"app_label": model_module.__name__.split('.')[-2]}

        new_class.add_to_class('_meta', Options(meta, new_class, **kwargs))

        for obj_name, obj in attrs.items():
            if isinstance(obj, Field):
                setattr(obj, 'attname', obj_name)
                setattr(obj, 'column', obj_name)
            new_class.add_to_class(obj_name, obj)
        if name not in ["NewBase", "DyModel"]:
            prefix = getattr(new_class._meta.meta, 'dynamo_table_prefix', None)
            if prefix:
                name = '{}_{}'.format(name, prefix)
            new_class.add_to_class('dyobjects', DynamoBase(new_class, name))

        return new_class

    def add_to_class(cls, name, value):
        setattr(cls, name, value)


class DyModel(six.with_metaclass(ModelBase)):
    """
    """
    def __init__(self, *args, **kwargs):
        # There is a rather weird disparity here; if kwargs, it's set, then args
        # overrides it. It should be one or the other; don't duplicate the work
        # The reason for the kwargs check is that standard iterator passes in by
        # args, and instantiation for iteration is 33% faster.
        args_len = len(args)
        if args_len > len(self._meta.fields):
            # Daft, but matches old exception sans the err msg.
            raise IndexError("Number of args exceeds number of fields")
        missing_fields = list()

        if not kwargs:
            fields_iter = iter(self._meta.fields.values())
            # The ordering of the zip calls matter - zip throws StopIteration
            # when an iter throws it. So if the first iter throws it, the second
            # is *not* consumed. We rely on this, so don't change the order
            # without changing the logic.
            for val, field in zip(args, fields_iter):
                setattr(self, field.attname, val)
        else:
            # Slower, kwargs-ready version.
            fields_iter = self._meta.fields
            klist = kwargs.values()

            for _f, field in fields_iter.items():
                setattr(self, field.attname, kwargs.get(_f))
                if field.attname in kwargs:
                    kwargs.pop(field.attname, None)
                else:
                    kwargs[field.attname] = None
                    missing_fields.append(field.attname)

        # Now we're left with the unprocessed fields that *must* come from
        # keywords, or default.
        fields_iter = iter(self._meta.fields.values())
        for field in fields_iter:
            # This slightly odd construct is so that we can access any
            # data-descriptor object (DeferredAttribute) without triggering its
            # __get__ method.
            if field.attname not in kwargs:
                # This field will be populated on request.
                continue
            if kwargs:
                try:
                    val = kwargs.pop(field.attname)
                    if not val and field.attname in missing_fields:
                        raise KeyError
                except KeyError:
                    # This is done with an exception rather than the
                    # default argument on pop because we don't want
                    # get_default() to be evaluated, and then not used.
                    # Refs #12057.
                    val = field.get_default()
                    if not val:
                        if isinstance(field, DateTimeField):
                            val = field.pre_save(self, True)
                            val = val.strftime('%Y-%m-%d')
            else:
                val = field.get_default()
            setattr(self, field.attname, val)

        if kwargs:
            for prop in list(kwargs):
                try:
                    if isinstance(getattr(self.__class__, prop), property):
                        setattr(self, prop, kwargs.pop(prop))
                except AttributeError:
                    pass
            if kwargs:
                raise TypeError("'%s' is an invalid keyword argument for this function" % list(kwargs)[0])
        super(DyModel, self).__init__()

    def clean_fields(self, exclude=None):
        """
        Cleans all fields and raises a ValidationError containing message_dict
        of all validation errors if any occur.
        """
        if exclude is None:
            exclude = []
        errors = {}
        for f in self._meta.fields.values():
            if f.attname in exclude:
                continue
            # Skip validation for empty fields with blank=True. The developer
            # is responsible for making sure they have a valid value.
            raw_value = getattr(self, f.attname)
            if f.blank and raw_value in f.empty_values:
                continue
            try:
                setattr(self, f.attname, f.clean(raw_value, self))
            except ValidationError as e:
                errors[f.attname] = e.error_list
        if errors:
            raise ValidationError(errors)

    def get_hash_key(self):
        """
        Description:
            Return hash key generated by uuid package
        """
        return uuid.uuid4().hex

    def get_changed_keys(self, old, new):
        """
        Description:
            Basically used in updation find changed key for which value has been changed
        Returns:
            all chnaged keys
        """
        fields = self._meta.fields
        change_keys = []
        for _f in fields:
            if getattr(old, _f) != getattr(new, _f):
                change_keys.append(_f)
        return change_keys

    def save(self):
        """
        Description:
            find if hash key is already existed or not
            if already exist calls update else create new item
        """
        hash_key_identifier = self._meta.hash_key_name
        if hasattr(self, hash_key_identifier):
            old_obj = self.__class__.dyobjects.get_item(getattr(self, hash_key_identifier))
            change_keys = self.get_changed_keys(old_obj, self)
            changed_dict = {}
            if change_keys:
                for each in change_keys:
                    changed_dict.update({each: getattr(self, each)})
            self.__class__.dyobjects.update_item(changed_dict)
        else:
            # create new
            data = {}
            for each in self._meta.fields:
                value = getattr(self, each)
                if value:
                    data.update({each: getattr(self, each)})
            self.__class__.dyobjects.create_item(data)
