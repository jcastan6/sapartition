from __future__ import division
import contextlib
import itertools
import operator

from sqlalchemy import func
from sqlalchemy.orm.exc import (
    NoResultFound,
    MultipleResultsFound,
)


# A backend is a tuple of a "pool" class and a "queue" class
# pool must have a imap_unordered method
# queue must have put and get methods
BACKENDS = {}
DEFAULT_BACKEND = None

DONE_EXCEPTIONS = (StopIteration,)
_QUERY_PART_DONE = object()

try:
    import time
    from multiprocessing.pool import ThreadPool
    from Queue import Queue as ThreadQueue
    from Queue import Empty as ThreadQueueEmpty
    DONE_EXCEPTIONS += (ThreadQueueEmpty,)
    THREADING_BACKEND = (ThreadPool, ThreadQueue, time.sleep)
    BACKENDS['threading'] = THREADING_BACKEND
    DEFAULT_BACKEND = THREADING_BACKEND
except ImportError:
    THREADING_BACKEND = None

try:
    import gevent
    from gevent.monkey import saved as gevent_patched
    from gevent.pool import Pool as GeventPool
    from gevent.pool import Group as GeventGroup
    from gevent.queue import Queue as GeventQueue
    from gevent.hub import LoopExit as GeventLoopExit
    DONE_EXCEPTIONS += (GeventLoopExit,)
    GEVENT_BACKEND = (GeventPool, GeventQueue, gevent.sleep)
    BACKENDS['gevent'] = GEVENT_BACKEND
    if 'socket' in gevent_patched or 'sys' in gevent_patched:
        DEFAULT_BACKEND = GEVENT_BACKEND
except ImportError:
    GEVENT_BACKEND = None


@contextlib.contextmanager
def no_context(query):
    yield query


@contextlib.contextmanager
def close_session_connection(query):
    try:
        yield query
    finally:
        query.session.remove()


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = itertools.tee(iterable)
    next(b, None)
    return itertools.izip(a, b)


def bounds_range(*args):
    the_range = xrange(*args)
    return pairwise(the_range)
    
    
def bounds_to_max(session, column, step=1000000, start=1):
    if start is None:
        start = session.query(func.min(column)).scalar()
    end = session.query(func.max(column)).scalar()
    return bounds_range(start, end, step)
    

def bounded_constraints(column, bounds):
    return [((column >= low) & (column < high)) for low, high in bounds]


def transformations_from_constraints(constraints):
    transformations = []
    for constraint in constraints:
        def binder(bound_constraint):
            return lambda query: query.filter(bound_constraint)
        transformations.append(binder(constraint))
    return transformations


def define_bounds(column, step=1000000, start=1):
    def factory(query, session=None):
        session = session or query.session
        bounds = bounds_to_max(session, column, step=step, start=start)
    return bound_factory


def define_bounds_transformations(column, step=1000000, start=1, end=None):
    def factory(query, session=None):
        session = session or query.session
        if end is None:
            bounds = bounds_to_max(session, column, step=step, start=start)
        else:
            bounds = bounds_range
        constraints = bounded_constraints(column, bounds)
        transformations = transformations_from_constraints(constraints)
        return transformations
    return factory


def apply_deferred_transformations(query, save=False):
    transformed = query
    if hasattr(query, '_deferred_transformations'):
        transformed = query._clone()
        for transformation in query._deferred_transformations:
            transformed = transformed.with_transformation(transformation)
        if query is not transformed and not save:
            transformed._deferred_transformations = ()
    return transformed


def deferred_transformation(transformation):
    def immediate_tranform(original):
        query = original._clone()
        setattr(query, '_deferred_transformations', 
                getattr(query, '_deferred_transformations', ()) + (transformation,))
        return query
    return immediate_tranform


#class DeferredTransformationsQueryMixin(object):
#    _deferred_transformations = ()
#
#    def with_deferred_transformation(self, transformation):
#        return self.with_transformation(deferred_transformation(transformation))
#
#    def apply_deferred_transformations(self):
#        return apply_deferred_transformations(self)
#
#    def __iter__(self):
#        if len(self._deferred_transformations) > 0:
#            new_self = self.apply_deferred_transformations()
#            return super(DeferredTransformationsQueryMixin, new_self).__iter__()
#        else:
#            return super(DeferredTransformationsQueryMixin, self).__iter__() 


class ParallelizableQueryMixin(object):
    def fork_with_transformations(self, transformations):
        return [self.with_transformation(transform) for transform in transformations]

    def parallelize_with_transformations(self, transformations, **kwargs):
        queries = self.fork_with_transformations(transformations)
        if len(queries) == 1:
            return queries[0]
        else:
            return ParallelizedQuery(queries, original=self, **kwargs)


class ParallelizedQuery(object):
    def __init__(self, queries, original, 
                 backend=DEFAULT_BACKEND, 
                 pool=None, 
                 spawn_context=no_context,
                 spawn_transformation=None,
                 swap_to_original_session=True,
                 preprocess_result=lambda query, row: row):
        self.queries = queries
        self.original_query = original

        if backend is None:
            raise NotImplementedError("Cannot find a backend to parallelize with")

        self._backend = backend
        self._spawn_transformation = spawn_transformation
        self._spawn_contextmanager = spawn_context

        self._swap_to_original_session = swap_to_original_session

        self._preprocess_result = preprocess_result
        self._pool = pool
        self._skip = None
        self._limit = None
        self._post_sort = False
        self._sort_columns = 0
        self._received = 0
        self._seen = 0
        self._sent = 0
        self._stop = False
        self._check_every = 100
        self._max_waiting_coefficient = 5
        self._executing = []
        self.__pool = None
        self.__tasks = None

    def _num_query_parts(self):
        return len(self.queries)

    def _spawnned_query_instance(self, query):
        if self._stop:
            query_context = no_context(None)
        else:
            if self._spawn_transformation is not None:
                query = query.with_transformation(self._spawn_transformation)
            query_context = self._spawn_contextmanager(query)
        return query_context

    def _spawn_callback_query_results(self, (input_query, callback)):
        _, _, sleeper = self._backend
        if self._stop:
            return 

        if self._limit is not None and not self._post_sort:
            def counting_callback(result):
                if self._seen >= self._limit:
                    raise StopIteration
                callback(result)
                self._seen += 1
            result_callback = counting_callback
        else:
            result_callback = callback

        with self._spawnned_query_instance(input_query) as query:
            query_idx = len(self._executing)
            self._executing.append(query)
            results = iter(query)
            num_queries = len(self.queries)
            for result in results:
                self._received += 1
                try:
                    preprocessed = self._preprocess_result(query, result)
                    result_callback(preprocessed)
                    if self._stop:
                        raise StopIteration
                    elif self._received % self._check_every == 0:
                        sleeper(0)
                except StopIteration:
                    results.close()
                    self._terminate()
                    break
            callback(_QUERY_PART_DONE)
            self._executing[query_idx] = None
            query.session.expunge_all()
    
    def _spawn_load_all_results(self, input_query):
        with self._spawnned_query_instance(input_query) as query:
            results = [self._preprocess_result(query, row) for row in query.all()]
        results.append(_QUERY_PART_DONE)
        return results

    def _discard_errors(self, fn):
        def wrapped(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                raise
                pass
        return wrapped

    def _terminate(self):
        self._stop = True

    def _cleanup(self):
        try:
           self._spawned_pool.kill()
        except Exception:
            pass
        try:
            self._spawned_tasks.kill()
        except Exception as e:
            pass
        for i, query in enumerate(self._executing):
            if query is None:
                continue
            try:
                query.session.bind.raw_connection().connection.cancel()
            except Exception as e:
                pass
            	 

    def _spawner(self, target, args=None):
        poolclass, _, _ = self._backend
        if self._pool is None:
            pool = poolclass()
        elif isinstance(self._pool, int):
            pool = poolclass(self._pool)
        else:
            pool = self._pool
        if args is None:
            args = self.queries
        self._spawned_pool = pool
        tasks = pool.imap_unordered(target, args)
        self._spawned_tasks = tasks
        def handler(thread):
            exc = thread.exception
            if exc in DONE_EXCEPTIONS:
                self._terminate()
                self._cleanup()
        if hasattr(tasks, 'link_exception'):
            tasks.link_exception(handler)
        return tasks, pool

    def _spawn_with_callback(self, callback):
        def gen_args():
            for query in self.queries:
                if self._stop:
                    break
                else:
                    yield (query, callback)
        runner = self._spawn_callback_query_results
        #catcher = self._discard_errors(runner)
        tasks, pool = self._spawner(runner, gen_args())
        return tasks, pool

    def _queue_spawner(self):
        _, queueclass, _ = self._backend
        results = queueclass(self._max_waiting_coefficient * len(self.queries))
        tasks, pool = self._spawn_with_callback(results.put)
        self._spawn_results = results
        return tasks, results, pool

    def all_per_query(self):
        tasks = self._spawner(self._spawn_load_all_results)
        return list(tasks)

    def all(self):
        per_query = self.all_per_query()
        return [result for results in per_query for result in results if result is not _QUERY_PART_DONE]

    def result_queue(self):
        tasks, results, pool = self._queue_spawner()
        return results

    def _iter_queue(self):
        finished = 0
        self._sent = 0
        expected = self._num_query_parts()
        self._spawned_impl = self._queue_spawner()
        tasks, results, pool = self._spawned_impl
        _, _, sleeper = self._backend
        NO_ITEM = object()
        while True:
            try:
                if finished >= expected:
                    break
                elif self._stop:
                    break
                item = results.get()
                if item is _QUERY_PART_DONE:
                    item = NO_ITEM
                    finished += 1
                    sleeper(0)
                    continue
                elif self._limit is not None and not self._post_sort:
                    if self._sent <= self._limit:
                        self._sent += 1
                    else:
                        break
                    self._stop = self._sent >= self._limit
                else:
                    self._sent += 1
                    while len(results.queue) >= results.maxsize:
                        yield item
                        self._sent += 1
                        item = results.get()
                        if item is _QUERY_PART_DONE:
                            item = NO_ITEM
                            finished += 1
                            sleeper(0)
                            break
                if not self._stop and item is not NO_ITEM:
                    yield item
                    item = NO_ITEM
                if self._sent % self._check_every == 0:
                    sleeper(0)
            except DONE_EXCEPTIONS as e:
                break
        self._cleanup()
        if self._stop and item is not NO_ITEM:
            yield item
            item = NO_ITEM

    def _iter_normal(self):
        finished = 0
        expected = self._num_query_parts()
        tasks, results = self._queue_spawner()
        for result in results:
            if result is _QUERY_PART_DONE:
                finished += 1
            else:
                yield result

    def __iter__(self):
        if hasattr(self._backend[1], '__iter__') and False:
            it = self._iter_normal()
        else:
            it = self._iter_queue()
        if self._swap_to_original_session:
            it = (self.session.merge(result, load=False) for result in it)
        if self._post_sort:
            it = iter(self._sorted_results(it))
        return it

    def new_with_parallel_config(self, queries, original_query=None, **kwargs):
        cls = type(self)
        if original_query is None:
            original_query = self.original_query
        inst = cls(queries, original_query)
        conf = self.__dict__.copy()
        conf.pop('queries')
        conf.pop('original_query')
        inst.__dict__.update(conf)
        inst.__dict__.update(kwargs)
        return inst

    @property
    def session(self):
        return self.original_query.session

    def slice(self, start, size):
        new = self._apply_over_queries('slice', start, size, init_kwargs={
            '_limit': size,
            '_skip': start,
        })
        return new

    def limit(self, limit):
        new = self.new_with_parallel_config(self.queries, _limit=limit)
        return new

    def offset(self, offset):
        new = self.new_with_parallel_config(self.queries, _skip=offset)
        return new

    def order_by(self, *args):
        new = self._apply_over_queries('order_by', *args, init_kwargs={
            '_post_sort': True,
            'original_query': self.original_query.order_by(*args),
        })
        newer = new.force_sort()
        return newer

    def force_sort(self, sort=True, max_results=None):
        if sort and hasattr(self.original_query, '_order_by'):
            sort_clauses = [getattr(item, 'element', item) for item in self.original_query._order_by]
            if hasattr(self.original_query, 'annotate'):
                keys = ['_parallel_query_sort_clause_{}'.format(idx) for idx in range(len(sort_clauses))]
                sort_columns = keys
                def transform(query):
                    for key, clause in zip(keys, sort_clauses):
                        query = query.annotate(key, clause)
                    return query
            else:
                sort_columns = len(sort_clauses)
                def transform(query):
                    for clause in sort_clauses:
                            query = query.add_column(clause)
                    return query
            init_kwargs = {
                '_post_sort': True,
                '_sort_columns': sort_columns,
            }
            new = self._apply_over_queries('with_transformation', transform, init_kwargs=init_kwargs)
            if max_results is not None:
                new = new.limit_children(max_results)
            return new
        else:
            return self.new_with_parallel_config(self.queries)

    def limit_children(self, limit):
        new = self._apply_over_queries('limit', limit, 
                                       init_kwargs={'_limit': limit})
        return new


    def first(self):
        results = iter(self.limit(1))
        found = next(results, None)
        return found

    def one(self):
        results = iter(self.limit(1))
        try:
            found = next(results)
            return found
        except StopIteration:
            raise NoResultFound

    def _apply_over_queries(self, name, *args, **kwargs):
        init_kwargs = kwargs.pop('init_kwargs', {})
        apply_proxy = lambda q: getattr(q, name)(*args, **kwargs)
        original_query = apply_proxy(self.original_query)
        queries = []
        for query in self.queries:
            queries.append(apply_proxy(query))
        parallel_query = self.new_with_parallel_config(queries, original_query, **init_kwargs)
        return parallel_query

    def _sorted_results(self, incomming):
        columns = self._sort_columns
        if isinstance(columns, int):
            assert columns > 0
            split_key = lambda row: (row[-columns:], row[:-columns])
        elif isinstance(columns, list):
            assert len(columns) > 0
            def split_key(item):
                key = []
                for column in columns:
                    key.append(getattr(item, column))
                    delattr(item, column)
                return tuple(key), item
        else:
            raise ValueError("Invalid post-sort column definition")
        def gen():
            keyed = (split_key(row) for row in incomming)
            for key, row in sorted(keyed, key=operator.itemgetter(0)):
                yield row
        it = gen()
        if self._limit is not None or self._skip is not None:
            start = self._skip or 0
            end = self._limit + start
            chunk = slice(start, end)
            it = list(it)
            it = it[chunk]
        return it
        
    def __getattr__(self, name):
        if not hasattr(self.original_query, name):
            raise AttributeError("{} not available in original query. Will not map over query parts".format(name))
        if not callable(getattr(self.original_query, name)):
            return getattr(self.original_query, name)
        def proxy(*args, **kwargs):
            original_fn = getattr(self.original_query, name)
            original_result = original_fn(*args, **kwargs)
            # If the function was an accessor instead of a chained builder
            if not isinstance(original_result, type(self.original_query)):
                return original_result
            return self._apply_over_queries(name, *args, **kwargs)
        return proxy

    def __repr__(self):
        return '<ParallelQuery: {0} parts over "{1!r}">'.format(self._num_query_parts(), self.original_query)

