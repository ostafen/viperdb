# ViperDB

[![codecov](https://codecov.io/gh/ostafen/viperdb/branch/main/graph/badge.svg?token=CXZTXRQ9YS)](https://codecov.io/gh/ostafen/viperdb)

ViperDB is a lightweight embedded key-value store written in pure Python. 
It has been designed for being extremely simple but efficient.

### Features

- **tiny**: the main db file consists of just 250 lines.
- **highly coverage**: thanks to the small codebase allows, almost every single line of code is tested.
- **log-structured**: ViperDB takes design concepts by log-structured databases such as [Bitcask](https://docs.riak.com/riak/kv/2.2.3/setup/planning/backend/bitcask/index.html).
- **written in pure Python**: no external dependency needed.

### API usage
```python
from viperdb import ViperDB

db = ViperDB('./db')
# db can be used as a simple dictionary
db['hello'] = 'ViperDB!'
print(db['hello'])
del db['hello']
```