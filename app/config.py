import argparse
from app.utils import generate_alphanumeric_string


def load_config():
    args = getArgs()
    return {
        "host": 'localhost',
        "port": args.port or 6379,
        "dir": args.dir,
        "dbfilename": args.dbfilename,
        "master_host": args.replicaof.split(' ')[0] if args.replicaof else None,
        "master_port": int(args.replicaof.split(' ')[1]) if args.replicaof else None,
        "is_replica": bool(args.replicaof),
        "master_replid": generate_alphanumeric_string(40) if not args.replicaof else '',
    }

def getArgs() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument('--port', type=int)
    parser.add_argument('--dir', type=str)
    parser.add_argument('--dbfilename', type=str)
    parser.add_argument('--replicaof', type=str)

    return parser.parse_args() or argparse.Namespace(port=6379, dir=None, dbfilename=None, replicaof=None)
