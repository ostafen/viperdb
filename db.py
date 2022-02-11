import base64
import datetime
import json
import os
import pickle
import threading
import zlib
from typing import Any, Dict
from dataclasses import dataclass


@dataclass
class ValuePointer:
    timestamp: float
    offset: int
    size: int
    encoding: str


def is_builtin_type(obj):
    return obj.__class__.__module__ == 'builtins'


def get_timestamp():
    return datetime.datetime.now().timestamp()


class Database:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._key_file = open(f'{path}/db.klog', 'a+')
        self._value_file = open(f'{path}/db.vlog', 'ba+')
        self._table = {}
        self._init_db()

    def _init_db(self):
        self._key_file.seek(0)

        json_record = self._key_file.readline()
        while json_record.strip() != '':
            record = json.loads(json_record)
            if record['type'] == 'set':
                self._table[record['key']] = ValuePointer(
                    timestamp=record['timestamp'],
                    offset=record['offset'],
                    size=record['size'],
                    encoding=record['encoding'],
                )
            else:
                del self._table[record['key']]

            json_record = self._key_file.readline()

    def _read_value(self, ptr: ValuePointer):
        self._value_file.seek(ptr.offset)
        encoded_value = self._value_file.read(ptr.size)
        return self._decode_value(encoded_value, ptr.encoding)

    def _seek_to_end(self):
        self._key_file.seek(0, os.SEEK_END)
        self._value_file.seek(0, os.SEEK_END)
        return self._value_file.tell()

    def _encode_value(self, value) -> bytes:
        if is_builtin_type(value):
            return json.dumps(value).encode('ascii')

        if type(value) != bytes:
            return pickle.dumps(value)

        return value

    def _get_encoding(self, value):
        if is_builtin_type(value):
            return 'json'
        elif type(value) == bytes:
            return 'bytes'
        return 'pickle'

    def _decode_value(self, encoded_value, encoding):
        if encoding == 'json':
            return json.loads(encoded_value)
        elif encoding == 'pickle':
            return pickle.loads(encoded_value)
        return encoded_value

    def _append_record(self, record: Dict[str, Any]):
        data = json.dumps(record)
        self._key_file.write(data + '\n')

    def _checksum(self, record, encoded_value=None):
        data = json.dumps(record).encode('ascii')
        if encoded_value is not None:
            data = data + b":" + encoded_value
        return zlib.crc32(data)

    def _get(self, key: str):
        ptr = self._table.get(key)
        if ptr is None:
            return None

        return self._read_value(ptr)

    def _set(self, key, value):
        offset = self._seek_to_end()
        encoded_value = self._encode_value(value)
        encoding = self._get_encoding(encoded_value)
        record = {
            'type': 'set',
            'timestamp': get_timestamp(),
            'key': key,
            'encoding': encoding,
            'offset': offset,
            'size': len(encoded_value)
        }
        record['checksum'] = self._checksum(record, encoded_value)
        self._append_record(record)
        self._value_file.write(encoded_value)
        self._table[key] = ValuePointer(
            timestamp=record['timestamp'],
            offset=offset,
            size=len(encoded_value),
            encoding=encoding
        )

    def _del(self, key):
        self._seek_to_end()
        record = {
            'type': 'del',
            'timestamp': get_timestamp(),
            'key': key
        }
        record['checksum'] = self._checksum(record)
        self._append_record(record)
        del self._table[key]

    def __getitem__(self, key: str):
        with self._lock:
            return self._get(key)

    def __setitem__(self, key: str, value: Any):
        with self._lock:
            self._set(key, value)

    def __delitem__(self, key: str):
        with self._lock:
            self._del(key)
