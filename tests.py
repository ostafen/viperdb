import datetime
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
