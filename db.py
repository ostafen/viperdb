import datetime
import json
import os
import pickle
import threading
import zlib
from typing import Any, Dict, Optional
from dataclasses import dataclass


@dataclass
class ValuePointer:
    timestamp: float
    expiration: float
    offset: int
    size: int
    encoding: str

    def is_expired(self) -> bool:
        if self.expiration < 0:
            return False
        return get_timestamp() > self.expiration


def is_builtin_type(obj):
    return obj.__class__.__module__ == 'builtins'


def get_timestamp():
    return datetime.datetime.now().timestamp()


class Database:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._path = path
        self._table = {}
        self._init_db()

    def _open_files(self):
        self._key_file = open(f'{self._path}/db.klog', 'a+')
        self._value_file = open(f'{self._path}/db.vlog', 'ba+')

    def _close_files(self):
        self._key_file.close()
        self._value_file.close()

    def _swap_files(self, new_key_file, new_value_file):
        self._close_files()

        os.rename(new_key_file.name, self._key_file.name)
        os.rename(new_value_file.name, self._value_file.name)

        self._key_file = new_key_file
        self._value_file = new_value_file

        self._close_files()
        self._open_files()

    def _init_db(self):
        os.makedirs(self._path, exist_ok=True)

        self._open_files()
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
                    expiration=-1 if 'expiration' not in record else record['expiration']
                )
            else:
                del self._table[record['key']]

            json_record = self._key_file.readline()

    def _read_value(self, ptr: ValuePointer, decode=True):
        self._value_file.seek(ptr.offset)
        encoded_value = self._value_file.read(ptr.size)
        if decode:
            return self._decode_value(encoded_value, ptr.encoding)
        return encoded_value

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

    def _is_none_or_expired(self, key):
        ptr = self._table.get(key)
        if ptr is None or ptr.is_expired():
            return True
        return False

    def _get(self, key: str):
        if self._is_none_or_expired(key):
            return None
        return self._read_value(self._table[key])

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
            encoding=encoding,
            expiration=-1
        )

    def _del(self, key):
        if self._is_none_or_expired(key):
            return None

        self._seek_to_end()
        timestamp = get_timestamp()
        record = {
            'type': 'del',
            'timestamp': timestamp,
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

    def _is_fresh(self, ptr: Optional[ValuePointer], record):
        return ptr is not None and ptr.timestamp == record['timestamp']

    def _reclaim(self):
        new_key_file = open(f'{self._path}/db.klog.tmp', 'a+')
        new_value_file = open(f'{self._path}/db.vlog.tmp', 'ba+')

        expired_keys = []
        for key, ptr in self._table.items():
            if ptr.is_expired():
                expired_keys.append(key)
                continue

            offset = new_value_file.tell()
            value = self._read_value(ptr, decode=False)
            new_value_file.write(value)

            new_record = {
                'type': 'set',
                'timestamp': ptr.timestamp,
                'key': key,
                'encoding': ptr.encoding,
                'offset': offset,
                'size': ptr.size,
            }
            if ptr.expiration >= 0:
                new_record['expiration'] = ptr.expiration
            new_record['checksum'] = self._checksum(new_record, value)
            ptr.offset = offset

            new_key_file.write(json.dumps(new_record) + '\n')

        self._swap_files(new_key_file, new_value_file)

        for key in expired_keys:
            del self._table[key]

    def reclaim(self):
        with self._lock:
            self._reclaim()
