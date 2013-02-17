#!./venv/bin/python
# -*- coding: utf-8 -*-

import os
import functools

from tornado.ioloop import IOLoop
from tornado import gen

import asyncdynamo


class DynamoException(Exception):

    pass


class ConcurrentUpdateException(DynamoException):

    pass


class PutException(DynamoException):

    pass


class QueryException(DynamoException):

    pass


class QueryChain(gen.Task):

    def __init__(self, table_proxy, key):
        super(QueryChain, self).__init__(self)
        self._table_proxy = table_proxy
        self._forward = True
        self._key = key
        self._range = None
        self._comp = None

    def gt(self, val):
        self._comp = "GT"
        self._range = val
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
                                   callback=callback)


class GenDynamo(object):

    def __init__(self, *args, **kwargs):
        self._db = asyncdynamo.AsyncDynamoDB(*args, **kwargs)

    def __getattr__(self, name):
        return GenTableProxy(self._db, name)


class GenTableProxy(object):

    def __init__(self, db, name):
        self._db = db
        self._table_name = name

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

    def _key(self, val):
        if isinstance(val, int):
            keytype = "N"
        elif isinstance(val, basestring):
            keytype = "S"
        else:
            raise ValueError("%r is not valid key for dynamo" % val)
        return {"HashKeyElement": {keytype: val}}

    def _new_rev_key(self):
        return os.urandom(2).encode("hex")

    def get(self, key, range_val=None):
        """
        returns item for given key
        """
        key = self._key(key)
        if range_val:
            key.update(RangeKeyElement=self._pack_val(range_val))
        return gen.Task(self._get, key)


    def _get(self, key, callback):
        cb = functools.partial(self._get_callback, callback)
        self._db.get_item(self._table_name, key, cb)

    def _get_callback(self, callback, response, error):
        if error:
            raise DynamoException((response or {}).get("message", None))
        if "Item" in response:
            callback(self._unpack(response["Item"]))
        else:
            callback(None)

    def batch_get(self, keys):
        return gen.Task(self._batch_get, keys)

    def _batch_get(self, keys, callback):
        items = {
            self._table_name: {
                "Keys": map(self._key, keys)
            }
        }
        cb = functools.partial(self._batch_get_callback, callback)
        self._db.batch_get_item(items, cb)

    def _batch_get_callback(self, callback, data, error):
        if error:
            raise DynamoException((data or {}).get("Message", None))
        items = data.get("Responses").get(self._table_name).get("Items")
        callback(map(self._unpack, items))

    def put(self, key, data):
        """
        puts data for given key into database
        """
        if "_rev" in data or "_rev_key" in data or "_id" in data:
            raise ValueError("new items should not contain `_id`, `_rev` or `_rev_key`")
        item = dict(data, _id=key, _rev=0, _rev_key=self._new_rev_key())
        expected = {"_id": {"Exists": False}}
        return gen.Task(self._put, item, expected)

    def update(self, data, item):
        """
        updates data for given item, item should first be retrieved by `get` method
        """
        if "_rev" not in item or "_rev_key" not in item or "_id" not in item:
            raise ValueError("item should contain `_id`, `_rev` and `_rev_key` attributes")
        rev = item["_rev"] + 1
        new_item = dict(data, _rev=rev, _rev_key=self._new_rev_key(), _id=item["_id"])
        expected = {"_id": {"Exists": True, "Value": self._pack_val(item["_id"])},
                    "_rev": {"Value": self._pack_val(item["_rev"])},
                    "_rev_key": {"Value": self._pack_val(item["_rev_key"])}}
        return gen.Task(self._put, new_item, expected)

    def _put(self, data, expected, callback):
        cb = functools.partial(self._put_callback, callback)
        self._db.put_item(self._table_name, self._pack(data), cb, expected)

    def _put_callback(self, callback, response, error):
        if error:
            if "#ConditionalCheckFailedException" in (response or {}).get("__type", ""):
                raise ConcurrentUpdateException()
            else:
                raise PutException((response or {}).get("message", None))
        callback()

    def query(self, key):
        return QueryChain(self, key)

    def _query_callback(self, callback, response, error):
        if "Items" in response:
            callback(map(self._unpack, response["Items"]))
        else:
            raise QueryException(error)
