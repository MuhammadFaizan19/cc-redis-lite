class Constants:
    PING = 'ping'
    ECHO = 'echo'
    GET = 'get'
    SET = 'set'
    DEL = 'del'
    CONFIG = 'config'
    KEYS = 'keys'
    INFO = 'info'
    REPL_CONF = 'replconf'
    PYSNC = 'psync'
    LISTENING_PORT = 'listening-port'
    REPLICATION = 'replication'
    PX = 'px'
    CAPABILITY = 'capa'
    MASTER = 'master'
    SLAVE = 'slave'
    OK = b'+OK\r\n'
    NULL = b'$-1\r\n'
    EMPTY_RDB = b'REDIS0011\xfa\tredis-ver\x057.2.0\xfa\nredis-bits\xc0@\xfa\x05ctime\xc2m\x08\xbce\xfa\x08used-mem\xc2\xb0\xc4\x10\x00\xfa\x08aof-base\xc0\x00\xff\xf0n;\xfe\xc0\xffZ\xa2'
