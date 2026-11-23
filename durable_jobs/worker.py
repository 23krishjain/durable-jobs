"""Allowlisted, deterministic local tasks only. No command execution or network."""
import json
import time

TASKS = {'sum_numbers'}


def validate(task, payload):
    if task not in TASKS:
        raise ValueError('unknown task')
    if not isinstance(payload, dict) or set(payload) != {'numbers'}:
        raise ValueError('sum_numbers expects only numbers')
    values = payload['numbers']
    if not isinstance(values, list) or len(values) > 1000 or any(type(n) is not int or abs(n) > 10**9 for n in values):
        raise ValueError('numbers must contain at most1000 bounded integers')


def run_one(queue, *, lease_seconds=30):
    job = queue.claim(lease_seconds=lease_seconds)
    if job is None:
        return None
    try:
        payload = json.loads(job['payload'])
        validate(job['task'], payload)
        result = {'total': sum(payload['numbers'])}
    except (ValueError, TypeError, KeyError):
        queue.fail(job['id'], job['token'], retry_delay=min(60, 2 ** (job['attempts'] - 1)))
    else:
        queue.complete(job['id'], job['token'], result)
    return job['id']
