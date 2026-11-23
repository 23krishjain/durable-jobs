import argparse
import json
from .queue import Queue
from .worker import run_one, validate


def main():
    parser = argparse.ArgumentParser(description='Local SQLite durable job queue demo')
    parser.add_argument('--db', default='jobs.sqlite3')
    sub = parser.add_subparsers(dest='command', required=True)
    submit = sub.add_parser('submit')
    submit.add_argument('--key', required=True)
    submit.add_argument('numbers', type=int, nargs='*')
    status = sub.add_parser('status')
    status.add_argument('id')
    sub.add_parser('work-once')
    args = parser.parse_args()
    queue = Queue(args.db)
    if args.command == 'submit':
        payload = {'numbers': args.numbers}
        validate('sum_numbers', payload)
        print(queue.submit('sum_numbers', payload, key=args.key))
    elif args.command == 'status':
        print(json.dumps(queue.get(args.id), indent=2))
    else:
        print(run_one(queue) or 'idle')


if __name__ == '__main__':
    main()
