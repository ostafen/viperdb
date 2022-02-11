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
        assert db[i] == i+1


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

    for i in range(n):
        if i % 2 == 0:
            del db[i]

    for i in range(n):
        if i % 2 == 0:
            assert db[i] is None
        else:
            assert db[i] == i+1

    db.reclaim()

    for i in range(n):
        if i % 2 == 0:
            assert db[i] is None
        else:
            assert db[i] == i+1
