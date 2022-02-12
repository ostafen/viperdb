import datetime
import os
import time

from db import ViperDB
import pytest
import shutil
from secrets import token_bytes


@pytest.fixture
def db(tmp_path):
    _db = ViperDB(tmp_path.name)
    yield _db
    _db.close()
    shutil.rmtree(tmp_path.name)


def test_insert_key(db: ViperDB):
    n = 1000
    for i in range(n):
        db[i] = i+1
        assert i in db
        assert db[i] == i+1


def get_expiration(seconds: int):
    return int((datetime.datetime.now() + datetime.timedelta(seconds=seconds)).timestamp()*1000)


def test_insert_with_expiration(db: ViperDB):
    expiration = get_expiration(1)
    expiration_long = get_expiration(10)

    n = 1000
    for i in range(n):
        db.set_with_expiration(i, i+1, expiration if i % 2 == 0 else expiration_long)
        assert db[i] == i + 1

    time.sleep(1)

    def run_check():
        for j in range(n):
            if j % 2 == 0:
                assert db[j] is None
            else:
                assert db[j] == j + 1

    run_check()

    db.reclaim()

    run_check()


class MyClass:
    pass


def test_insert_class(db: ViperDB):
    o = MyClass()
    o.field = 'myField'
    db['myKey'] = o
    assert db['myKey'].field == 'myField'


def test_insert_raw_bytes(db: ViperDB):
    data = token_bytes(1000)
    db['data'] = data
    assert db['data'] == data


def test_remove_key(db: ViperDB):
    n = 1000
    for i in range(n):
        db[i] = i + 1

    # delete even keys
    for i in range(n):
        if i % 2 == 0:
            del db[i]

    def run_check():
        for i in range(n):
            if i % 2 == 0:
                assert i not in db
            else:
                assert db[i] == i+1

    run_check()

    db._reopen()

    run_check()

    db.reclaim()

    run_check()


def test_recover_database1(db: ViperDB):
    n = 1000
    for i in range(n):
        db[i] = i+1

    key_file = db._key_file
    key_file.seek(0)

    new_key_file = open(key_file.name + '.damaged', 'a+')

    # copy all but the last entry
    json_entry = key_file.readline()
    last_entry = json_entry
    while json_entry.strip() != '':
        last_entry = json_entry
        json_entry = key_file.readline()

        if json_entry.strip() != '':
            new_key_file.write(last_entry)

    damaged_entry = last_entry.strip()[:(len(last_entry)//2)]    # take the half of the entry
    new_key_file.write(damaged_entry + '\n')

    new_key_file.flush()
    new_key_file.close()
    os.rename(new_key_file.name, key_file.name)

    db.close()

    # create the .OPEN file, so that repair is triggered
    with open(db._open_filename(), 'w'):
        pass

    db._init_db()

    for i in range(n-1):
        assert db[i] == i+1

    assert db[n-1] is None


def test_recover_database2(db: ViperDB):
    n = 1000
    for i in range(n):
        db[i] = i+1

    ptr = db._table[n - 1]

    value_file = db._value_file
    value_file.close()
    value_file = open(f'{db._path}/db.vlog', 'br+')
    value_file.seek(ptr.offset)
    value_file.write(b'abcd')

    db._value_file = value_file

    db.close()

    # create the .OPEN file, so that repair is triggered
    with open(db._open_filename(), 'w'):
        pass

    db._init_db()

    for i in range(n-1):
        assert db[i] == i+1

    assert db[n-1] is None
