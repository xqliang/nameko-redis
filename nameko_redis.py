from urlparse import parse_qs
from urlparse import urlparse

from nameko.extensions import DependencyProvider

from redis import StrictRedis as _StrictRedis
from redis.sentinel import Sentinel

REDIS_URIS_KEY = 'REDIS_URIS'


class Redis(DependencyProvider):
    def __init__(self, key):
        self.key = key
        self.client = None

    def setup(self):
        redis_uris = self.container.config[REDIS_URIS_KEY]
        self.redis_uri = redis_uris[self.key]

    def start(self):
        conf = self.parse_uri(self.redis_uri)
        conf.setdefault("decode_responses", True)
        scheme = conf.pop("scheme")
        if scheme == "redis-sentinel":
            service_name = conf.pop("service_name", None)
            if not service_name:
                raise Exception("`service_name` is required for "
                                "redis-sentinel scheme")
            prefer_master = conf.pop("prefer_master", True)
            sentinels = conf.pop("sentinels")
            sentinel = Sentinel(sentinels, **conf)
            if prefer_master:
                self.client = sentinel.master_for(service_name)
            else:
                self.client = sentinel.slave_for(service_name)
        else:
            decode_responses = conf["decode_responses"]
            self.client = _StrictRedis.from_url(
                    self.redis_uri, decode_responses=decode_responses)

    def parse_uri(self, uri):
        """
        >>> rd = Redis('dev')
        >>> conf = rd.parse_uri('redis-sentinel://:pass@host1,host2:26379/0?'
        ...                     'service_name=dev&prefer_master=true&'
        ...                     'socket_timeout=3')
        >>> conf == {
        ...     "scheme": "redis-sentinel",
        ...     "sentinels": [("host1", 26379), ("host2", 26379)],
        ...     "db": 0,
        ...     "password": "pass",
        ...     "service_name": "dev",
        ...     "prefer_master": True,
        ...     "socket_timeout": 3.0,
        ... }
        True
        """
        parsed = urlparse(uri)
        sentinels = []
        for netloc in parsed.netloc.split(","):
            addr = netloc.rsplit("@", 1)[-1]
            if ':' in addr:
                host, port = addr.split(':', 1)
            else:
                host, port = addr, 26379
            sentinels.append((host, int(port)))
        res = dict(scheme=parsed.scheme, sentinels=sentinels)
        if parsed.password is not None:
            res['password'] = parsed.password
        if parsed.path[1:]:
            res['db'] = int(parsed.path[1:])
        float_keys = ("socket_timeout", "socket_connect_timeout")
        int_keys = ("socket_read_size", "min_other_sentinels")
        bool_keys = ("prefer_master", "socket_keepalive", "retry_on_timeout",
                     "decode_responses")
        for key, values in parse_qs(parsed.query).items():
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
            res[key] = value
        return res

    def stop(self):
        self.client = None

    def kill(self):
        self.client = None

    def get_dependency(self, worker_ctx):
        return self.client
