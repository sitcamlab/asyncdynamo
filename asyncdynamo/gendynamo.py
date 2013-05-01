#!./venv/bin/python
# -*- coding: utf-8 -*-

import os
import functools
import types
import json

from tornado.ioloop import IOLoop
from tornado import gen

import asyncdynamo


class DynamoException(Exception):
    pass


class ConcurrentUpdateException(DynamoException):
    pass


class PutException(DynamoException):
    pass


class RemoveException(DynamoException):
    pass


class QueryException(DynamoException):
    pass


class ScanException(DynamoException):
    pass


class ScanChain(gen.Task):

    def __init__(self, table_proxy):
        super(ScanChain, self).__init__(self)
        self._table_proxy = table_proxy
        self._forward = True
        self._scan = None
        self._comp = None
        self._limit = None

    def gt(self, val):
        self._comp = "GT"
        self._scan = val
        return self

    def lt(self, val):
        self._comp = "LT"
        self._scan = val
        return self

    def asc(self):
        self._forward = True
        return self

    def desc(self):
        self._forward = False
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    def __call__(self, callback):
        callback = functools.partial(self._table_proxy._scan_callback, callback)
        if self._scan:
            scan_filter = {
                "AttributeValueList": [self._table_proxy._pack_val(self._scan)],
                "ComparisonOperator": self._comp
            }
        else:
            scan_filter = None

        self._table_proxy._db.scan(self._table_proxy._table_name,
                                   limit=self._limit,
                                   scan_filter=scan_filter,
                                   callback=callback)



class QueryChain(gen.Task):

    def __init__(self, table_proxy, key):
        super(QueryChain, self).__init__(self)
        self._table_proxy = table_proxy
        self._forward = True
        self._key = key
        self._range = None
        self._comp = None
        self._limit = None

    def gt(self, val):
        self._comp = "GT"
        self._range = val
        return self

    def limit(self, limit):
        self._limit = limit
        return self

    def lt(self, val):
        self._comp = "LT"
        self._range = val
        return self

    def asc(self):
        self._forward = True
        return self

    def desc(self):
        self._forward = False
        return self

    def __call__(self, callback):

        if None in [self._key, self._range, self._comp]:
            raise RuntimeError("QueryChain wan't not configured properly")

        key = self._table_proxy._pack_val(self._key)

        callback = functools.partial(self._table_proxy._query_callback, callback)

        range_key_conditions = {
            "AttributeValueList": [self._table_proxy._pack_val(self._range)],
            "ComparisonOperator": self._comp
        }

        self._table_proxy._db.query(self._table_proxy._table_name, key,
                                   range_key_conditions=range_key_conditions,
                                   scan_index_forward=self._forward,
                                   limit=self._limit,
                                   callback=callback)


class GetMixin(object):


    def get(self, **kwargs):
        hash_key, range_key, rest = self._extract_keys(kwargs)
        if rest:
            raise KeyError("%r arguments are not supported "
                           "for `get` method" % rest)

        key = self._key(hash_key, range_key)
        return gen.Task(self._get, key)


    def _get(self, key, callback):
        cb = functools.partial(self._get_callback, callback)
        self._db.get_item(self._table_name, key, cb)


    def _get_callback(self, callback, response, error):
        self._check_error(response, error)
        if "Item" in response:
            callback(self._unpack(response.get("Item")))
        else:
            callback(None)


class BatchGetMixin(object):


    def batch_get(self, items):
        keys = []
        for item in items:
            hash_key, range_key, rest = self._extract_keys(item)
            if rest:
                raise KeyError("%r arguments are not supported "
                               "for `batch_get` method" % rest)
            keys.append(self._key(hash_key, range_key))
        get_items = {
            self._table_name: {
                "Keys": keys
            }
        }
        return gen.Task(self._batch_get, get_items)


    def _batch_get(self, get_items, callback):
        cb = functools.partial(self._batch_get_callback, callback)
        self._db.batch_get_item(get_items, cb)


    def _batch_get_callback(self, callback, response, error):
        self._check_error(response, error)
        items = response.get("Responses").get(self._table_name).get("Items")
        callback(map(self._unpack, items))


class IncrementMixin(object):


    def increment(self, **kwargs):
        hash_key, range_key, rest = self._extract_keys(kwargs)
        key = self._key(hash_key, range_key)
        update_data = {}
        for field, increment in rest.items():
            update_data[field] = {"Value": self._pack_val(increment),
                                  "Action": "ADD"}
        return gen.Task(self._increment, key, update_data)


    def _increment(self, key, update_data, callback):
        cb = functools.partial(self._increment_callback, callback)
        self._db.update_item(self._table_name, key, update_data, cb)


    def _increment_callback(self, callback, response, error):
        self._check_error(response, error)
        callback(self._unpack(response.get("Attributes")))


class PutMixin(object):


    def put(self, **kwargs):
        self._extract_keys(kwargs)

        if self.range_key_name:
            expected = {self.range_key_name: {"Exists": False}}
        else:
            expected = {self.hash_key_name: {"Exists": False}}

        data = self._pack(kwargs)
        return gen.Task(self._put, data, expected)


    def _put(self, data, expected, callback):
        cb = functools.partial(self._put_callback, callback)
        self._db.put_item(self._table_name, data, cb, expected)


    def _put_callback(self, callback, response, error):
        self._check_error(response, error, cls=PutException)
        callback(response.get("ConsumedCapacityUnits"))


class UpdateMixin(object):

    def update(self, **kwargs):
        hash_key, range_key, rest = self._extract_keys(kwargs)
        key = self._key(hash_key, range_key)
        update_data = {}
        for field, value in rest.items():
            update_data[field] = {"Value": self._pack_val(value),
                                  "Action": "PUT"}
        return gen.Task(self._update, key, update_data)

    def _update(self, key, update_data, callback):
        cb = functools.partial(self._update_callback, callback)
        self._db.update_item(self._table_name, key, update_data, cb)

    def _update_callback(self, callback, response, error):
        self._check_error(response, error)
        callback(self._unpack(response.get("Attributes")))


class MassWriteMixin(object):

    def mass_write(self, items):
        if len(items) > 25:
            raise RuntimeError("mass_write accepts not more than 25 items")
        items = map(self._pack, items)
        return gen.Task(self._mass_write, items)

    def _mass_write(self, items, callback):
        self._db.make_request("BatchWriteItem", body=json.dumps({
            "RequestItems": {
                self._table_name: [
                    {"PutRequest": {"Item": item}}
                    for item in items
                ]
            }
        }), callback=functools.partial(self._mass_write_callback, callback))

    def _mass_write_callback(self, callback, response, error):
        self._check_error(response, error)
        callback(response.get("Responses", {}))


class RemoveMixin(object):


    def remove(self, **kwargs):
        hash_key, range_key, rest = self._extract_keys(kwargs)
        if rest:
            raise KeyError("%r arguments are not supported "
                           "for `get` method" % rest)

        key = self._key(hash_key, range_key)

        expected = {}
        for attr, value in kwargs.items():
            expected[attr] = {"Exists": True, "Value": self._pack_val(value)}

        return gen.Task(self._remove, key, expected)


    def _remove(self, key, expected, callback):
        cb = functools.partial(self._remove_callback, callback)
        self._db.remove_item(self._table_name, key, cb, expected)


    def _remove_callback(self, callback, response, error):
        self._check_error(response, error, cls=RemoveException)
        callback(response.get("ConsumedCapacityUnits"))


class ScanMixin(object):

    def scan(self):
        return ScanChain(self)

    def _scan_callback(self, callback, response, error):
        self._check_error(response, error, cls=ScanException)
        callback(map(self._unpack, response.get("Items")))


class QueryMixin(object):


    def query(self, key):
        return QueryChain(self, key)


    def _query_callback(self, callback, response, error):
        self._check_error(response, error, cls=QueryException)
        callback(map(self._unpack, response.get("Items")))


class GenDynamoTable(GetMixin, BatchGetMixin, IncrementMixin,
                     PutMixin, QueryMixin, RemoveMixin, ScanMixin,
                     UpdateMixin, MassWriteMixin):


    def __init__(self, hash_key, range_key=None):
        self.hash_key_type, self.hash_key_name = hash_key
        if range_key:
            self.range_key_type, self.range_key_name = range_key
        else:
            self.range_key_type = None
            self.range_key_name = None
        
        if self.hash_key_type not in (int, str):
            raise TypeError("hash_key should be int or str")
        
        if self.range_key_type not in (int, str, None):
            raise TypeError("range_key should be int or str")


    def _check_error(self, response, error, cls=None):
        if error:
            response = response or {}
            message = response.get("message") or response.get("Message")
            if cls is None:
                cls = DynamoException
                if "#ConditionalCheckFailedException" in response.get("__type", ""):
                    cls = ConcurrentUpdateException
            raise cls(message)


    def _extract_keys(self, data):
        data = data.copy()

        if self.hash_key_name is None:
            raise KeyError("hash key '%s' not provided" % self.hash_key_name)
        
        hash_key = data.pop(self.hash_key_name)

        if self.hash_key_type is int and not isinstance(hash_key, int):
            raise ValueError("'%s' should be int but %r provided" %
                             (self.hash_key_name, hash_key))
        
        if self.hash_key_type is str and not isinstance(hash_key, basestring):
            raise ValueError("'%s' should be string but %r provided" %
                             (self.hash_key_name, hash_key))

        range_key = None
        if self.range_key_name:

            if self.range_key_name not in data:
                raise KeyError("range key '%s' not provided" %
                               self.range_key_name)

            range_key = data.pop(self.range_key_name)

            if self.range_key_type is int and not isinstance(range_key, int):
                raise ValueError("'%s' should be int but %r provided" %
                                 (self.range_key_name, range_key))

            if self.range_key_type is str and\
                         not isinstance(range_key, basestring):
                raise ValueError("'%s' should be string but %r provided" %
                                 (self.range_key_name, range_key))

        return hash_key, range_key, data


    def _unpack_val(self, val):
        if "N" in val:
            return int(val["N"])
        elif "S" in val:
            return val["S"]
        elif "SS" in val:
            return set(val["SS"])
        elif "SN" in val:
            return set(map(int, val["SS"]))
        else:
            raise ValueError("can not unpack %r", val)


    def _pack_val(self, val):
        if isinstance(val, int):
            keytype = "N"
            val = str(val)
        elif isinstance(val, basestring):
            keytype = "S"
        elif isinstance(val, set):
            if not len(val):
                raise ValueError("empty sets are not supported by DynamoDB")
            for item in val:
                if isinstance(item, int):
                    itemtype = "N"
                elif isinstance(item, str):
                    itemtype = "S"
                else:
                    raise ValueError("set should contain only `int` or `basestring` items")
                break
            if itemtype == "N":
                for item in val:
                    if not isinstance(item, int):
                        raise ValueError("set should contain values of same type")
                val = map(set, val)
            elif itemtype == "S":
                for item in val:
                    if not isinstance(item, basestring):
                        raise ValueError("set should contain values of same type")
            val = list(val)
            keytype = itemtype + "S"
        else:
            raise ValueError("can not pack %r" % val)
        return {keytype: val}


    def _unpack(self, item):
        return dict((k, self._unpack_val(v)) for k, v in item.items())


    def _pack(self, item):
        return dict((k, self._pack_val(v)) for k, v in item.items())


    def _key(self, hash_key, range_key=None):
        key = {"HashKeyElement": self._pack_val(hash_key)}
        if range_key is not None:
            key.update(RangeKeyElement=self._pack_val(range_key))
        return key



class GenDynamo(object):


    class __metaclass__(type):
        def __new__(cls, name, bases, dct):
            tables = []
            for name, attr in dct.items():
                if isinstance(attr, GenDynamoTable):
                    tables.append(name)
            dct.update(_tables=tables)
            return type.__new__(cls, name, bases, dct)


    def __init__(self, *args, **kwargs):
        self._db = asyncdynamo.AsyncDynamoDB(*args, **kwargs)
        for name in self._tables:
            table = getattr(self, name)
            table._db = self._db
            table._table_name = name
        self._pack = getattr(self, self._tables[0])._pack

    def multi_write(self, **tables):
        data = {}
        count = 0
        for table, items in tables.items():
            if table not in self._tables:
                raise RuntimeError("unknown table %r" % table)
            data[table] = [{"PutRequest": {"Item": self._pack(item)}}
                           for item in items]
            count += len(data[table])
            if count > 25:
                raise RuntimeError("multi_write accepts not more than 25 items")
        return gen.Task(self._multi_write, data)

    def multi_delete(self, **tables):
        data = {}
        count = 0
        for table, items in tables.items():
            tbl = getattr(self, table)
            del_requests = []
            for item in items:
                hash_key, range_key, rest = tbl._extract_keys(item)
                if rest:
                    raise RuntimeError("%r can't be handled by multi_delete" % rest)
                del_requests.append({"DeleteRequest": {"Key": tbl._key(hash_key, range_key)}})
                count += 1
                if count > 25:
                    raise RuntimeError("multi_delete accepts not more than 25 items")
            data[table] = del_requests
        return gen.Task(self._multi_write, data)

    def _multi_write(self, data, callback):
        self._db.make_request("BatchWriteItem", body=json.dumps({
            "RequestItems": data
        }), callback=functools.partial(self._multi_write_callback, callback))

    def _multi_write_callback(self, callback, response, error):
        callback(response.get("Responses", {}))
