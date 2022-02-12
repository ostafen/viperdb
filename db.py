import datetime
import json
import os
import pickle
import threading
import zlib
from typing import Any, Dict, Hashable, Optional
from dataclasses import dataclass, fields, asdict


@dataclass
class ValuePointer:
    timestamp: float
    expiration: Optional[float]
    offset: int
    size: int
    encoding: str

    @classmethod
    def from_entry(cls, entry):
        class_fields = {f.name for f in fields(cls)}
        return ValuePointer(**{k: entry.get(k) for k in class_fields})

    def as_dict(self):
        _dict = asdict(self)
        return {k: v for k, v in _dict.items() if v is not None}

    def is_expired(self) -> bool:
        if self.expiration is None:
            return False
        return get_timestamp() > self.expiration


def is_builtin_type(obj):
    return obj.__class__.__module__ == 'builtins'


def get_timestamp():
    return int(datetime.datetime.now().timestamp() * 1000)


DB_OPEN_FILE = '.OPEN'


class ViperDB:
    def __init__(self, path: str):
        self._lock = threading.Lock()
        self._path = path
        self._table = {}
        self._init_db()

    def _open_filename(self):
        return f'{self._path}/{DB_OPEN_FILE}'

    def _reopen(self):
        self._close()
        self._init_db()

    def _open_files(self):
        self._key_file = open(f'{self._path}/db.klog', 'a+')
        self._value_file = open(f'{self._path}/db.vlog', 'ba+')

    def _flush(self):
        self._key_file.flush()
        self._value_file.flush()

    def _close_files(self):
        self._flush()
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

    def _check_db_open(self) -> bool:
        db_open_filename = self._open_filename()
        is_open = os.path.exists(db_open_filename)
        if not is_open:
            with open(db_open_filename, 'w'):
                pass
        return is_open

    def _init_db(self):
        os.makedirs(self._path, exist_ok=True)
        self._open_files()

        if self._check_db_open():
            self._repair_db()

        self._key_file.seek(0)

        json_entry = self._key_file.readline()
        while json_entry.strip() != '':
            entry = json.loads(json_entry)
            if entry['type'] == 'set':
                self._table[entry['key']] = ValuePointer.from_entry(entry)
            else:
                del self._table[entry['key']]

            json_entry = self._key_file.readline()

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
        if type(value) == bytes:
            return value

        if is_builtin_type(value):
            return json.dumps(value).encode('ascii')

        return pickle.dumps(value)

    def _get_encoding(self, value):
        if type(value) == bytes:
            return 'bytes'
        elif is_builtin_type(value):
            return 'json'
        return 'pickle'

    def _decode_value(self, encoded_value, encoding):
        if encoding == 'json':
            return json.loads(encoded_value)
        elif encoding == 'pickle':
            return pickle.loads(encoded_value)
        return encoded_value

    def _append_entry(self, entry: Dict[str, Any]):
        data = json.dumps(entry)
        self._key_file.write(data + '\n')

    def _checksum(self, entry, encoded_value=None):
        data = json.dumps(entry, sort_keys=True).encode('ascii')
        if encoded_value is not None:
            data = data + b":" + encoded_value
        return zlib.crc32(data)

    def _is_none_or_expired(self, key):
        ptr = self._table.get(key)
        if ptr is None or ptr.is_expired():
            return True
        return False

    def _get(self, key: Hashable):
        if self._is_none_or_expired(key):
            return None
        return self._read_value(self._table[key])

    def _set(self, key: Hashable, value: Any, expiration=None):
        offset = self._seek_to_end()
        encoded_value = self._encode_value(value)
        encoding = self._get_encoding(value)
        entry = {
            'type': 'set',
            'timestamp': get_timestamp(),
            'key': key,
            'encoding': encoding,
            'offset': offset,
            'size': len(encoded_value),
            **({'expiration': expiration} if expiration is not None else {})
        }
        entry['checksum'] = self._checksum(entry, encoded_value)

        self._append_entry(entry)
        self._value_file.write(encoded_value)
        self._table[key] = ValuePointer.from_entry(entry)

    def set_with_expiration(self, key: Hashable, value: Any, expiration: int):
        with self._lock:
            self._set(key, value, expiration=expiration)

    def _del(self, key: Hashable):
        if self._is_none_or_expired(key):
            return None

        self._seek_to_end()
        timestamp = get_timestamp()
        entry = {
            'type': 'del',
            'timestamp': timestamp,
            'key': key
        }
        entry['checksum'] = self._checksum(entry)
        self._append_entry(entry)
        del self._table[key]

    def __getitem__(self, key: Hashable):
        with self._lock:
            return self._get(key)

    def __setitem__(self, key: Hashable, value: Any):
        with self._lock:
            self._set(key, value)

    def __delitem__(self, key: Hashable):
        with self._lock:
            self._del(key)

    def __contains__(self, key: Hashable):
        with self._lock:
            return not self._is_none_or_expired(key)

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

            ptr.offset = offset
            new_entry = ptr.as_dict()
            new_entry['checksum'] = self._checksum(new_entry, value)

            new_key_file.write(json.dumps(new_entry) + '\n')

        self._swap_files(new_key_file, new_value_file)

        for key in expired_keys:
            del self._table[key]

    def reclaim(self):
        with self._lock:
            self._reclaim()

    def _close(self):
        self._close_files()
        os.remove(f'{self._path}/{DB_OPEN_FILE}')
        self._table.clear()

    def close(self):
        with self._lock:
            self._close()

    def _repair_db(self):
        new_key_file = open(f'{self._path}/db.klog.tmp', 'a+')
        new_value_file = open(f'{self._path}/db.vlog.tmp', 'ba+')

        new_key_file.truncate()
        new_value_file.truncate()

        self._key_file.seek(0)

        json_entry = self._key_file.readline()

        while json_entry.strip() != '':
            try:
                entry = json.loads(json_entry)
                encoded_value = None
                if entry['type'] == 'set':
                    encoded_value = self._read_value(ValuePointer.from_entry(entry), decode=False)

                checksum = self._checksum({k: v for k, v in entry.items() if k != 'checksum'}, encoded_value)

                if checksum != entry['checksum']:
                    break

                new_key_file.write(json_entry)
                new_value_file.write(encoded_value)
            except (json.JSONDecodeError, IOError):
                break

            json_entry = self._key_file.readline()

        self._swap_files(new_key_file, new_value_file)
