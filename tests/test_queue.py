import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from durable_jobs.queue import Queue, Conflict, LostLease
from durable_jobs.worker import run_one, validate


class QueueTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'jobs.db'
        self.now = 100.
        self.q = Queue(self.path, clock=lambda: self.now)

    def submit(self, key='k', **kwargs):
        return self.q.submit('sum_numbers', {'numbers': [1, 2]}, key=key, **kwargs)

    def test_persisted_reopen(self):
        ident = self.submit()
        self.assertEqual(Queue(self.path).get(ident)['state'], 'queued')

    def test_idempotent_submission(self):
        self.assertEqual(self.submit(), self.submit())

    def test_conflicting_payload(self):
        self.submit()
        with self.assertRaises(Conflict):
            self.q.submit('sum_numbers', {'numbers': [9]}, key='k')

    def test_claim_excludes_other_worker(self):
        self.submit()
        self.assertIsNotNone(self.q.claim())
        self.assertIsNone(self.q.claim())

    def test_complete_and_no_token_in_status(self):
        ident = self.submit()
        job = self.q.claim()
        self.q.complete(ident, job['token'], {'total': 3})
        self.assertEqual(self.q.get(ident)['result'], {'total': 3})
        self.assertNotIn('token', self.q.get(ident))
        self.assertIsNone(self.q.claim())

    def test_wrong_token(self):
        ident = self.submit()
        self.q.claim()
        with self.assertRaises(LostLease):
            self.q.complete(ident, 'wrong', None)

    def test_expired_owner_cannot_complete(self):
        ident = self.submit()
        job = self.q.claim(lease_seconds=2)
        self.now += 2
        with self.assertRaises(LostLease):
            self.q.complete(ident, job['token'], None)
        next_job = self.q.claim()
        self.assertNotEqual(next_job['token'], job['token'])
        self.assertEqual(next_job['attempts'], 2)

    def test_stale_owner_after_reclaim(self):
        ident = self.submit()
        old = self.q.claim(lease_seconds=1)
        self.now += 2
        new = self.q.claim()
        with self.assertRaises(LostLease):
            self.q.fail(ident, old['token'])
        self.q.complete(ident, new['token'], 3)

    def test_backoff_blocks_until_ready(self):
        ident = self.submit()
        job = self.q.claim()
        self.q.fail(ident, job['token'], retry_delay=5)
        self.assertIsNone(self.q.claim())
        self.now += 5
        self.assertEqual(self.q.claim()['id'], ident)

    def test_retry_budget_dead_letters(self):
        ident = self.submit(max_attempts=1)
        job = self.q.claim()
        self.q.fail(ident, job['token'])
        self.assertEqual(self.q.get(ident)['state'], 'dead')
        self.now += 100
        self.assertIsNone(self.q.claim())

    def test_expired_last_attempt_dead_letters(self):
        ident = self.submit(max_attempts=1)
        self.q.claim(lease_seconds=1)
        self.now += 1
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.get(ident)['state'], 'dead')

    def test_concurrent_claims_unique(self):
        ids = {self.submit(str(i)) for i in range(12)}
        with ThreadPoolExecutor(max_workers=6) as pool:
            jobs = list(pool.map(lambda _: self.q.claim(), range(18)))
        claimed = [j['id'] for j in jobs if j]
        self.assertEqual(len(claimed), 12)
        self.assertEqual(set(claimed), ids)

    def test_concurrent_idempotency(self):
        with ThreadPoolExecutor(max_workers=6) as pool:
            ids = list(pool.map(lambda _: self.submit(), range(12)))
        self.assertEqual(len(set(ids)), 1)

    def test_worker_runs_allowlisted_sum(self):
        ident = self.submit()
        self.assertEqual(run_one(self.q), ident)
        self.assertEqual(self.q.get(ident)['result'], {'total': 3})

    def test_worker_unknown_task_does_not_execute(self):
        ident = self.q.submit('shell', {'command': 'nothing'}, key='bad', max_attempts=1)
        run_one(self.q)
        self.assertEqual(self.q.get(ident)['state'], 'dead')

    def test_validation(self):
        for payload in [{'numbers': [True]}, {'numbers': [1.2]}, {'numbers': [10**10]}, {'numbers': [], 'extra': 1}]:
            with self.subTest(payload=payload), self.assertRaises(ValueError):
                validate('sum_numbers', payload)

    def test_bad_limits(self):
        for value in [0, -1, True, float('inf'), float('nan')]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                self.q.claim(lease_seconds=value)
        for value in [0, 21, True, 1.5]:
            with self.assertRaises(ValueError):
                self.submit(max_attempts=value)

    def test_large_or_nan_payload_rejected(self):
        for payload in ['x' * 70000, {'n': float('nan')}]:
            with self.assertRaises(ValueError):
                self.q.submit('x', payload, key='k')

    def test_missing_status(self):
        with self.assertRaises(KeyError):
            self.q.get('missing')

    def test_transaction_rolls_back(self):
        ident = self.submit()
        with self.assertRaises(RuntimeError):
            with self.q._transaction() as con:
                con.execute('DELETE FROM jobs')
                raise RuntimeError()
        self.assertEqual(self.q.get(ident)['state'], 'queued')

    def test_independent_process_claims(self):
        ids = {self.submit(str(i)) for i in range(4)}
        code = "import sys;from durable_jobs.queue import Queue;j=Queue(sys.argv[1],clock=lambda:100.).claim();print(j['id'] if j else 'idle')"
        children = [subprocess.Popen([sys.executable, '-c', code, str(self.path)], stdout=subprocess.PIPE, text=True) for _ in range(8)]
        outputs = []
        for child in children:
            out, _ = child.communicate(timeout=10)
            self.assertEqual(child.returncode, 0)
            if out.strip() != 'idle':
                outputs.append(out.strip())
        self.assertEqual(len(outputs), 4)
        self.assertEqual(set(outputs), ids)

    def test_subprocess_crash_recovery(self):
        ident = self.submit()
        # Actual process exits without acknowledgement after a claim. A real
        # lease timestamp is deterministically advanced by a test-only clock.
        code = '''import os,sys
from durable_jobs.queue import Queue
q=Queue(sys.argv[1],clock=lambda:100.)
assert q.claim(lease_seconds=2)
os._exit(23)
'''
        proc = subprocess.run([sys.executable, '-c', code, str(self.path)], timeout=10)
        self.assertEqual(proc.returncode, 23)
        self.now = 103.
        restarted = Queue(self.path, clock=lambda: self.now)
        job = restarted.claim()
        self.assertEqual(job['id'], ident)
        self.assertEqual(job['attempts'], 2)
        restarted.complete(ident, job['token'], {'total': 3})
        self.assertEqual(restarted.get(ident)['state'], 'succeeded')


if __name__ == '__main__':
    unittest.main()
