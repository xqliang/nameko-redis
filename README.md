# nameko-redis
[![PyPI version](https://badge.fury.io/py/nameko-redis.svg)](https://badge.fury.io/py/nameko-redis)
[![Build Status](https://travis-ci.org/etataurov/nameko-redis.svg?branch=master)](https://travis-ci.org/etataurov/nameko-redis)

Redis dependency for nameko services

## Installation
```
pip install nameko-redis
```

## Usage
```python
from nameko.rpc import rpc
from nameko_redis import Redis


class MyService(object):
    name = "my_service"

    redis = Redis('development')
    redis2 = Redis('dev_via_sentinel')

    @rpc
    def hello(self, name):
        self.redis.set("foo", name)
        return "Hello, {}!".format(name)

    @rpc
    def bye(self):
        name = self.redis2.get("foo")
        return "Bye, {}!".format(name)
```
To specify redis connection string you will need a config
```yaml
AMQP_URI: 'pyamqp://guest:guest@localhost'
REDIS_URIS:
 development: 'redis://localhost:6379/0'
 dev_via_sentinel: 'redis-sentinel://localhost:26379,localhost:26380/0?service_name=dev'
```
