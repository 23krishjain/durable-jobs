from pathlib import Path
import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor

from durable_jobs.queue import Queue, LostLease, CancelRequested


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / 'jobs.db'
        self.now = 100.
        self.q = Queue(self.path, clock=lambda: self.now)

    def job(self, key='k', **kw):
        return self.q.submit('sum_numbers', {'numbers': [1]}, key=key, **kw)


class RenewTests(Base):
    def test_renew_extends_from_now(self):
        self.job()
        j = self.q.claim(lease_seconds=10)
        self.now = 108
        self.assertEqual(self.q.renew(j['id'], j['token'], lease_seconds=10), 118)
        self.now = 112  # past the original expiry, inside the renewed one
        self.q.complete(j['id'], j['token'], {'total': 1})
        self.assertEqual(self.q.get(j['id'])['state'], 'succeeded')

    def test_renewed_lease_still_expires(self):
        self.job()
        j = self.q.claim(lease_seconds=10)
        self.q.renew(j['id'], j['token'], lease_seconds=5)
        self.now = 105
        with self.assertRaises(LostLease):
            self.q.renew(j['id'], j['token'])

    def test_stale_owner_cannot_renew_after_reclaim(self):
        self.job()
        old = self.q.claim(lease_seconds=5)
        self.now = 106
        new = self.q.claim(lease_seconds=5)
        self.assertEqual(new['attempts'], 2)
        with self.assertRaises(LostLease):
            self.q.renew(old['id'], old['token'])
        self.q.renew(new['id'], new['token'])

    def test_renew_validates_seconds(self):
        self.job()
        j = self.q.claim()
        for bad in (0, -1, float('nan'), True):
            with self.assertRaises(ValueError):
                self.q.renew(j['id'], j['token'], lease_seconds=bad)


class CancelTests(Base):
    def test_queued_cancel_is_immediate_and_never_claimed(self):
        ident = self.job()
        self.assertEqual(self.q.cancel(ident), 'canceled')
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.get(ident)['state'], 'canceled')

    def test_running_cancel_is_cooperative(self):
        ident = self.job()
        j = self.q.claim(lease_seconds=10)
        self.assertEqual(self.q.cancel(ident), 'running')
        self.assertTrue(self.q.get(ident)['cancel_requested'])
        with self.assertRaises(CancelRequested):
            self.q.renew(j['id'], j['token'])
        self.q.acknowledge_cancel(j['id'], j['token'])
        self.assertEqual(self.q.get(ident)['state'], 'canceled')
        self.assertIsNone(self.q.claim())

    def test_complete_wins_if_owner_finishes_before_acknowledging(self):
        ident = self.job()
        j = self.q.claim()
        self.q.cancel(ident)
        self.q.complete(j['id'], j['token'], {'total': 1})
        self.assertEqual(self.q.get(ident)['state'], 'succeeded')
        self.assertEqual(self.q.cancel(ident), 'succeeded')

    def test_crashed_owner_with_cancel_request_is_not_retried(self):
        ident = self.job(max_attempts=5)
        self.q.claim(lease_seconds=5)
        self.q.cancel(ident)
        self.now = 110
        self.assertIsNone(self.q.claim())
        self.assertEqual(self.q.get(ident)['state'], 'canceled')

    def test_cancel_after_failure_requeue_cancels_directly(self):
        ident = self.job()
        j = self.q.claim()
        self.q.fail(j['id'], j['token'], retry_delay=5)
        self.assertEqual(self.q.cancel(ident), 'canceled')

    def test_ack_without_request_and_stale_ack_rejected(self):
        ident = self.job()
        j = self.q.claim(lease_seconds=5)
        with self.assertRaises(ValueError):
            self.q.acknowledge_cancel(j['id'], j['token'])
        self.q.cancel(ident)
        self.now = 106
        with self.assertRaises(LostLease):
            self.q.acknowledge_cancel(j['id'], j['token'])

    def test_cancel_is_idempotent_and_unknown_raises(self):
        ident = self.job()
        self.q.cancel(ident)
        self.assertEqual(self.q.cancel(ident), 'canceled')
        with self.assertRaises(KeyError):
            self.q.cancel('missing')

    def test_cancel_racing_claim_has_one_consistent_outcome(self):
        # Each round: one thread claims, another cancels. Either order is legal,
        # but the final state must match what the claimer observed.
        for i in range(25):
            ident = self.job(key='r%d' % i)
            with ThreadPoolExecutor(2) as pool:
                claim = pool.submit(self.q.claim)
                cancel = pool.submit(self.q.cancel, ident)
                claimed, outcome = claim.result(), cancel.result()
            state = self.q.get(ident)
            if outcome == 'canceled':
                self.assertEqual(state['state'], 'canceled')
                self.assertTrue(claimed is None or claimed['id'] != ident)
            else:
                self.assertEqual(outcome, 'running')
                self.assertEqual(claimed['id'], ident)
                self.assertTrue(state['cancel_requested'])
                self.q.acknowledge_cancel(ident, claimed['token'])
            self.assertEqual(self.q.get(ident)['state'], 'canceled')


class MigrationTests(unittest.TestCase):
    def test_day_one_database_upgrades_in_place(self):
        with tempfile.TemporaryDirectory() as d:
            path = Path(d) / 'old.db'
            con = sqlite3.connect(path)
            con.execute('''CREATE TABLE jobs (
                id TEXT PRIMARY KEY, idem TEXT NOT NULL UNIQUE,
                task TEXT NOT NULL, payload TEXT NOT NULL,
                state TEXT NOT NULL CHECK(state IN ('queued','running','succeeded','dead')),
                attempts INTEGER NOT NULL, max_attempts INTEGER NOT NULL,
                available REAL NOT NULL, lease_until REAL, token TEXT,
                result TEXT, error TEXT, created REAL NOT NULL)''')
            con.execute("INSERT INTO jobs VALUES ('a','k','sum_numbers','{\"numbers\":[2]}','queued',0,3,1,NULL,NULL,NULL,NULL,1)")
            con.commit(); con.close()
            q = Queue(path, clock=lambda: 50.)
            self.assertEqual(q.cancel('a'), 'canceled')
            self.assertEqual(q.get('a')['payload'], {'numbers': [2]})
            Queue(path)  # reopening a migrated database is a no-op


if __name__ == '__main__':
    unittest.main()
