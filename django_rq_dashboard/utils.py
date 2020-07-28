from itertools import groupby

import redis
from django.conf import settings
from rq import Connection, Queue, Worker

try:
    from rq_scheduler import Scheduler
except ImportError:
    # rq_scheduler is not installed
    Scheduler = None

def is_authenticated(user):
    if callable(user.is_authenticated):
        return user.is_authenticated()
    return user.is_authenticated


def by_name(obj):
    return obj.name


def get_rq_admin_context():  # Helper for get_context_data
    opts = getattr(settings, 'RQ', {}).copy()
    opts.pop('eager', None)

    with Connection(connection=redis.Redis(**opts)) as connection:
        ctx = {}
        ctx.update({
            'queues': sorted(Queue.all(connection=connection),
                             key=by_name),
            'workers': sorted(Worker.all(connection=connection),
                              key=by_name),
        })
        if Scheduler:
            scheduler = Scheduler(connection)
            get_queue = lambda job: job.origin
            all_jobs = sorted(scheduler.get_jobs(), key=get_queue)
            ctx['scheduler'] = scheduler
            ctx['scheduled_queues'] = [
                {'name': queue, 'job_count': len(list(jobs))}
                for queue, jobs in groupby(all_jobs, get_queue)]
        return ctx


def get_redis_connection():
    opts = getattr(settings, 'RQ', {}).copy()
    opts.pop('eager', None)

    with Connection(connection=redis.Redis(**opts)) as connection:
        return connection
