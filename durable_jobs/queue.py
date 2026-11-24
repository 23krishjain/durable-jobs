"""SQLite-backed local job queue. At-least-once attempts, not exactly-once effects."""
from contextlib import contextmanager
import json
import math
import sqlite3
import time
import uuid


class Conflict(ValueError):
    pass


class LostLease(RuntimeError):
    pass


class CancelRequested(RuntimeError):
    """Raised to a lease holder when renewal finds a cancellation request."""


def positive(value, name):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        raise ValueError(name + ' must be positive and finite')


class Queue:
    def __init__(self, path, *, clock=time.time):
        self.path = str(path)
        if self.path == ':memory:':
            raise ValueError('use an on-disk database; connections are short-lived')
        self.clock = clock
        with self._transaction() as con:
            con.execute('''CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY, idem TEXT NOT NULL UNIQUE,
                task TEXT NOT NULL, payload TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','dead','canceled')),
                attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
                available REAL NOT NULL, lease_until REAL, token TEXT,
                result TEXT, error TEXT, created REAL NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0)''')
            self._migrate(con)
            con.execute('CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state, available)')

    @staticmethod
    def _migrate(con):
        # Day-1 databases lack the 'canceled' state and cancel_requested column.
        sql = con.execute("SELECT sql FROM sqlite_master WHERE name='jobs'").fetchone()[0]
        if "'canceled'" not in sql:
            con.execute('ALTER TABLE jobs RENAME TO jobs_old')
            con.execute('DROP INDEX IF EXISTS jobs_ready')
            con.execute('''CREATE TABLE jobs (
                id TEXT PRIMARY KEY, idem TEXT NOT NULL UNIQUE,
                task TEXT NOT NULL, payload TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','dead','canceled')),
                attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
                available REAL NOT NULL, lease_until REAL, token TEXT,
                result TEXT, error TEXT, created REAL NOT NULL,
                cancel_requested INTEGER NOT NULL DEFAULT 0)''')
            con.execute('''INSERT INTO jobs SELECT id,idem,task,payload,state,attempts,max_attempts,
                available,lease_until,token,result,error,created,0 FROM jobs_old''')
            con.execute('DROP TABLE jobs_old')
        con.execute('CREATE INDEX IF NOT EXISTS jobs_ready ON jobs(state, available)')

    @contextmanager
    def _transaction(self):
        con = sqlite3.connect(self.path, timeout=5, isolation_level=None)
        con.row_factory = sqlite3.Row
        try:
            con.execute('PRAGMA busy_timeout=5000')
            con.execute('PRAGMA synchronous=FULL')
            con.execute('BEGIN IMMEDIATE')
            yield con
            con.commit()
        except BaseException:
            con.rollback()
            raise
        finally:
            con.close()

    def _now(self):
        value = self.clock()
        if not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError('clock must return finite seconds')
        return value

    @staticmethod
    def _json(value):
        encoded = json.dumps(value, sort_keys=True, separators=(',', ':'), allow_nan=False)
        if len(encoded.encode()) > 65536:
            raise ValueError('JSON exceeds 64KiB')
        return encoded

    def submit(self, task, payload, *, key, max_attempts=3):
        if not isinstance(task, str) or not task or len(task) > 100:
            raise ValueError('task must be 1-100 characters')
        if not isinstance(key, str) or not key or len(key) > 200:
            raise ValueError('key must be 1-200 characters')
        if type(max_attempts) is not int or not 1 <= max_attempts <= 20:
            raise ValueError('max_attempts must be 1-20')
        body, now = self._json(payload), self._now()
        with self._transaction() as con:
            old = con.execute('SELECT * FROM jobs WHERE idem=?', (key,)).fetchone()
            if old:
                if (old['task'], old['payload'], old['max_attempts']) != (task, body, max_attempts):
                    raise Conflict('idempotency key already names a different request')
                return old['id']
            ident = str(uuid.uuid4())
            con.execute('''INSERT INTO jobs
                (id,idem,task,payload,state,attempts,max_attempts,available,created)
                VALUES (?,?,?,?,'queued',0,?,?,?)''',
                (ident, key, task, body, max_attempts, now, now))
            return ident

    def claim(self, *, lease_seconds=30):
        positive(lease_seconds, 'lease_seconds')
        now = self._now()
        with self._transaction() as con:
            # Expired attempts consume budget. Requeue or dead-letter atomically.
            con.execute('''UPDATE jobs SET
                state=CASE WHEN cancel_requested THEN 'canceled'
                           WHEN attempts>=max_attempts THEN 'dead' ELSE 'queued' END,
                token=NULL, lease_until=NULL, available=?, error='lease expired'
                WHERE state='running' AND lease_until<=?''', (now, now))
            row = con.execute('''SELECT * FROM jobs WHERE state='queued' AND available<=?
                ORDER BY available,created,id LIMIT 1''', (now,)).fetchone()
            if row is None:
                return None
            token = str(uuid.uuid4())
            con.execute("UPDATE jobs SET state='running',attempts=attempts+1,token=?,lease_until=? WHERE id=?",
                        (token, now + lease_seconds, row['id']))
            return dict(con.execute('SELECT * FROM jobs WHERE id=?', (row['id'],)).fetchone())

    def _owned(self, con, ident, token, now):
        row = con.execute("SELECT * FROM jobs WHERE id=? AND state='running' AND token=? AND lease_until>?",
                          (ident, token, now)).fetchone()
        if row is None:
            raise LostLease('job is missing, no longer owned or lease expired')
        return row

    def complete(self, ident, token, result):
        encoded, now = self._json(result), self._now()
        with self._transaction() as con:
            self._owned(con, ident, token, now)
            con.execute("UPDATE jobs SET state='succeeded',result=?,token=NULL,lease_until=NULL,error=NULL WHERE id=?",
                        (encoded, ident))

    def fail(self, ident, token, *, retry_delay=1):
        positive(retry_delay, 'retry_delay')
        now = self._now()
        with self._transaction() as con:
            row = self._owned(con, ident, token, now)
            state = 'dead' if row['attempts'] >= row['max_attempts'] else 'queued'
            con.execute('''UPDATE jobs SET state=?,available=?,error='task failed',
                token=NULL,lease_until=NULL WHERE id=?''', (state, now + retry_delay, ident))

    def renew(self, ident, token, *, lease_seconds=30):
        """Extend a live lease from now. Raises CancelRequested if cancel was asked for."""
        positive(lease_seconds, 'lease_seconds')
        now = self._now()
        with self._transaction() as con:
            row = self._owned(con, ident, token, now)
            if row['cancel_requested']:
                raise CancelRequested('cancellation requested; stop and call acknowledge_cancel')
            con.execute('UPDATE jobs SET lease_until=? WHERE id=?', (now + lease_seconds, ident))
            return now + lease_seconds

    def cancel(self, ident):
        """Queued jobs cancel at once. Running jobs only get a request; the owner stops
        cooperatively. Finished jobs are unchanged. Returns the resulting state."""
        with self._transaction() as con:
            row = con.execute('SELECT * FROM jobs WHERE id=?', (ident,)).fetchone()
            if row is None:
                raise KeyError(ident)
            if row['state'] == 'queued':
                con.execute("UPDATE jobs SET state='canceled',error='canceled' WHERE id=?", (ident,))
                return 'canceled'
            if row['state'] == 'running':
                con.execute('UPDATE jobs SET cancel_requested=1 WHERE id=?', (ident,))
            return row['state']

    def acknowledge_cancel(self, ident, token):
        """Owner confirms it stopped. Needs the current unexpired token and a request."""
        now = self._now()
        with self._transaction() as con:
            row = self._owned(con, ident, token, now)
            if not row['cancel_requested']:
                raise ValueError('no cancellation was requested')
            con.execute("UPDATE jobs SET state='canceled',token=NULL,lease_until=NULL,error='canceled' WHERE id=?",
                        (ident,))

    def get(self, ident):
        with self._transaction() as con:
            row = con.execute('SELECT * FROM jobs WHERE id=?', (ident,)).fetchone()
            if row is None:
                raise KeyError(ident)
            out = dict(row)
            # Ownership tokens are internal capabilities, not status fields.
            del out['token']
            out['cancel_requested'] = bool(out['cancel_requested'])
            out['payload'] = json.loads(out['payload'])
            out['result'] = json.loads(out['result']) if out['result'] is not None else None
            return out
