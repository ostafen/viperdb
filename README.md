# ViperDB :snake:

[![codecov](https://codecov.io/gh/ostafen/viperdb/branch/main/graph/badge.svg?token=CXZTXRQ9YS)](https://codecov.io/gh/ostafen/viperdb)

ViperDB is a lightweight embedded key-value store written in pure Python. 
It has been designed for being extremely simple while efficient.

### Features

- **tiny**: the main db file consists of just ~300 lines of code.
- **highly coverage**: thanks to the small codebase, every single line of code is tested.
- **log-structured**: ViperDB takes design concepts by log-structured databases such as [Bitcask](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html).
- **written in pure Python**: no external dependency needed.

### Installation

```bash
foo@bar:~$ pip3 install viperdb
```

### Python version
ViperDB has been tested with Python 3.8.

### Database layout

ViperDB simply consists of two files: a **key log file** and a **value log file**.
The first is used to maintain information about values (e.g. offset, size, etc...) which are actually stored in the value log.
This layout allows to speed-up db initialization, which consists in loading the pointers to the entire key-space from the key-file to a dictionary. 
For simplicity, the key file is treated as a text file, with each line containing a json-encoded entry.
The value file is viewed as a raw sequence of bytes. Before being written to the value file, each value is encoded according to the following scheme:
builtin types (except for the **bytes** type) are json-encoded, while user-defined classes are pickled.

To keep logic simple, no automatic compaction is performed in the background: unused space must be reclaimed explicitly through the **reclaim** function.

### API usage
```python
from viperdb import ViperDB

db = ViperDB('./db')
# db can be used as a simple dictionary
db['hello'] = 'ViperDB!'
assert db['hello'] == 'ViperDB'

del db['hello']
assert 'hello' not in db

db.reclaim() # call this method periodically to free unused space.
db.close() # flush any pending write and close the database.
```

### Contribute

ViperDB is a very recent project and, as such, it comes with no warranty.
If you find any bug, or have some suggestion, feel free to contribute by opening a new issue or making a pull request.