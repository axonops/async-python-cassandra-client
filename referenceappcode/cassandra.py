from ssl import PROTOCOL_TLS, SSLContext, CERT_REQUIRED
from threading import Lock
from typing import Optional

from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, Session
from cassandra.policies import ExponentialReconnectionPolicy, DCAwareRoundRobinPolicy, TokenAwarePolicy
from cassandra.query import dict_factory

from .retry_policy import LimitedRetryPolicy
from ...config import config

_mu: Lock = Lock()
_cluster: Optional[Cluster] = None
_session: Optional[Session] = None


def get_session(reconnect=False) -> Session:
    with _mu:
        if reconnect or _session is None:
            connect()
        return _session


def connect():
    global _cluster
    global _session

    close()

    # Configure authentication
    if config.cassandra_username == '':
        auth_provider = None
    else:
        auth_provider = PlainTextAuthProvider(username=config.cassandra_username, password=config.cassandra_password)

    # Configure TLS options
    sslctx = None
    if config.cassandra_use_tls:
        sslctx = SSLContext(PROTOCOL_TLS)
        if config.cassandra_tls_ca:
            sslctx.load_verify_locations(config.cassandra_tls_ca)
            sslctx.verify_mode = CERT_REQUIRED
        if config.cassandra_tls_crt and config.cassandra_tls_key:
            sslctx.load_cert_chain(certfile=config.cassandra_tls_crt, keyfile=config.cassandra_tls_key)

    contact_points = []
    for host_port in config.cassandra_hosts:
        contact_points.append(host_port.split(':')[0])

    _cluster = Cluster(
        contact_points=contact_points,
        port=config.cassandra_port,
        auth_provider=auth_provider,
        reconnection_policy=ExponentialReconnectionPolicy(0.01, 0.2, 64),
        default_retry_policy=LimitedRetryPolicy(),
        ssl_context=sslctx,
        load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc=config.cassandra_dc)))

    _session = _cluster.connect(keyspace=config.cassandra_keyspace)
    _session.row_factory = dict_factory


def close():
    global _cluster
    global _session

    if _session is not None:
        _session.shutdown()
        _session = None
    if _cluster is not None:
        _cluster.shutdown()
        _cluster = None
