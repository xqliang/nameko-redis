from urlparse import parse_qs
from urlparse import urlparse

from nameko.extensions import DependencyProvider

from redis import StrictRedis as _StrictRedis
from redis.connection import UnixDomainSocketConnection, SSLConnection
from redis.sentinel import Sentinel


REDIS_URIS_KEY = 'REDIS_URIS'
SCHEME_SENTINEL = 'redis-sentinel'
SCHEME_UNIX = 'unix'
SCHEME_REDISS = 'rediss'


class Redis(DependencyProvider):
    def __init__(self, key):
        self.key = key
        self.client = None

    def setup(self):
        redis_uris = self.container.config[REDIS_URIS_KEY]
        self.redis_uri = redis_uris[self.key]

    def start(self):
        conf = self.parse_uri(self.redis_uri)
        conf['init_kwargs'].setdefault("decode_responses", True)
        if conf["scheme"] == SCHEME_SENTINEL:
            if not conf.get("service_name"):
                raise Exception("`service_name` is required for "
                                "{} scheme".format(SCHEME_SENTINEL))
            sentinel = Sentinel(conf["address"], **conf["init_kwargs"])
            if conf["prefer_master"]:
                self.client = sentinel.master_for(conf["service_name"])
            else:
                self.client = sentinel.slave_for(conf["service_name"])
        else:
            self.client = _StrictRedis(**conf["init_kwargs"])

    def parse_uri(self, uri):
        """
        >>> rd = Redis('dev')
        >>> conf = rd.parse_uri('redis-sentinel://:pass@host1,host2:26379/0?'
        ...                     'service_name=dev&prefer_master=true&'
        ...                     'socket_timeout=3')
        >>> conf == {
        ...     "scheme": "redis-sentinel",
        ...     "address": [("host1", 26379), ("host2", 26379)],
        ...     "service_name": "dev",
        ...     "prefer_master": True,
        ...     "init_kwargs": {
        ...         "db": 0,
        ...         "password": "pass",
        ...         "socket_timeout": 3.0,
        ...     }
        ... }
        True
        """
        url = urlparse(uri)
        res = {
            'scheme': url.scheme,
            'address': [],
            'init_kwargs': {
                'db': int(url.path[1:]),
                'password': url.password,
            },
        }

        for netloc in url.netloc.split(","):
            addr = netloc.rsplit("@", 1)[-1]
            if ':' in addr:
                host, port = addr.split(':', 1)
            elif url.scheme == SCHEME_SENTINEL:
                host, port = addr, 26379
            else:
                host, port = addr, 6379
            res['address'].append((host, int(port)))

        float_keys = ("socket_timeout", "socket_connect_timeout")
        int_keys = ("db", "socket_read_size", "min_other_sentinels")
        bool_keys = ("prefer_master", "socket_keepalive", "retry_on_timeout",
                     "decode_responses")
        for key, values in parse_qs(url.query).items():
            if key in float_keys:
                value = float(values[0])
            elif key in int_keys:
                value = int(values[0])
            elif key in bool_keys:
                if values[0] == '':
                    value = None
                elif values[0].upper() in ('0', 'F', 'FALSE', 'N', 'NO'):
                    value = False
                else:
                    value = bool(values[0])
            else:
                value = values[0]
            res['init_kwargs'][key] = value

        if url.scheme == SCHEME_SENTINEL:
            res.update({
                'service_name': res['init_kwargs'].pop('service_name', None),
                'prefer_master': res['init_kwargs'].pop('prefer_master', True)
            })
        elif url.scheme == SCHEME_UNIX:
            res['init_kwargs'].update({
                'path': url.path,
                'connection_class': UnixDomainSocketConnection,
            })
        else:
            res['init_kwargs'].update({
                'host': url.hostname,
                'port': int(url.port or 6379),
            })
            if url.scheme == SCHEME_REDISS:
                res['init_kwargs']['connection_class'] = SSLConnection

        return res

    def stop(self):
        self.client = None

    def kill(self):
        self.client = None

    def get_dependency(self, worker_ctx):
        return self.client
