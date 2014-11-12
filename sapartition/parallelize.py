import contextlib
import itertools

from sqlalchemy import func


# A backend is a tuple of a "pool" class and a "queue" class
# pool must have a imap_unordered method
# queue must have put and get methods
BACKENDS = {}
DEFAULT_BACKEND = None

DONE_EXCEPTIONS = (StopIteration,)
_QUERY_PART_DONE = object()

try:
    from multiprocessing.pool import ThreadPool
    from Queue import Queue as ThreadQueue
    from Queue import Empty as ThreadQueueEmpty
    DONE_EXCEPTIONS += (ThreadQueueEmpty,)
    THREADING_BACKEND = (ThreadPool, ThreadQueue)
    BACKENDS['threading'] = THREADING_BACKEND
    DEFAULT_BACKEND = THREADING_BACKEND
except ImportError:
    THREADING_BACKEND = None

try:
    from gevent.monkey import saved as gevent_patched
    from gevent.pool import Pool as GeventPool
    from gevent.queue import Queue as GeventQueue
    from gevent.hub import LoopExit as GeventLoopExit
    DONE_EXCEPTIONS += (GeventLoopExit,)
    GEVENT_BACKEND = (GeventPool, GeventQueue)
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
        query.session.connection().close()


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
    def factory(session):
        bounds = bounds_to_max(session, column, step=step, start=start)
    return bound_factory


def define_bounds_transformations(column, step=1000000, start=1, end=None):
    def factory(session):
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
        for transformation in query._deferred_transformations:
            transformed = transformed.with_transformation(transformation)
        if query is not transformed and not save:
            transformed._deferred_transformations = ()
    return transformed


class ParallelizableQueryMixin(object):
    _deferred_transformations = ()

    def with_deferred_transformation(self, transformation):
        query = self.filter()
        query._deferred_transformations += (transformation,)
        return query

    def apply_deferred_transformations(self):
        return apply_deferred_transformations(self)

    def fork_with_transformations(self, transformations):
        return [self.with_transformation(transform) for transform in transformations]

    def parallelize_with_transformations(self, transformations, **kwargs):
        queries = self.fork_with_transformations(transformations)
        return ParallelizedQuery(queries, original=self, **kwargs)


class ParallelizedQuery(object):
    def __init__(self, queries, original, 
                 backend=DEFAULT_BACKEND, 
                 pool=None, 
                 spawn_context=no_context,
                 spawn_transformation=None,
                 swap_to_original_session=True,
                 preprocess_result=lambda row: row):
        self.queries = queries
        self.original_query = original

        if backend is None:
            raise NotImplementedError("Cannot find a backend to parallelize with")

        self._backend = backend
        self._spawn_transformation = spawn_transformation
        self._spawn_contextmanager = spawn_context

        if swap_to_original_session:
            _preprocess_result = preprocess_result
            def preprocess_result(query, row):
                # There HAS to be a better way to add these results to an 
                # existing session safely
                try:
                    query.session.expunge(row)
                except ProgrammingError:
                    pass
                try:
                    self.original_query.session.add(row)
                except AssertionError:
                    # This is probably not the right thing to do
                    pass
                return _preprocess_result(row)

        self._preprocess_result = preprocess_result
        self._pool = pool
        self._limit = None

    def _num_query_parts(self):
        return len(self.queries)

    def _spawnned_query_instance(self, query):
        if self._spawn_transformation is not None:
            query = query.with_transformation(self._spawn_transformation)
        query_context = self._spawn_contextmanager(query)
        return query_context

    def _spawn_callback_query_results(self, (input_query, callback)):
        with self._spawnned_query_instance(input_query) as query:
            for result in query:
                callback(self._preprocess_result(query, result))
            callback(_QUERY_PART_DONE)
    
    def _spawn_load_all_results(self, input_query):
        with self._spawnned_query_instance(input_query) as query:
            results = [self._preprocess_result(query, row) for row in query.all()]
        results.append(_QUERY_PART_DONE)
        return results

    def _spawner(self, target, args=None):
        poolclass, _ = self._backend
        if self._pool is None:
            pool = poolclass()
        elif isinstance(self._pool, int):
            pool = poolclass(self._pool)
        else:
            pool = self._pool
        if args is None:
            args = self.queries
        tasks = pool.imap_unordered(target, args)
        return tasks

    def _spawn_with_callback(self, callback):
        args = [(query, callback) for query in self.queries]
        tasks = self._spawner(self._spawn_callback_query_results, args)
        return tasks

    def _queue_spawner(self):
        _, queueclass = self._backend
        results = queueclass()
        tasks = self._spawn_with_callback(results.put)
        return tasks, results

    def all_per_query(self):
        tasks = self._spawner(self._spawn_load_all_results)
        return list(tasks)

    def all(self):
        per_query = self.all_per_query()
        return [result for results in per_query for result in results if result is not _QUERY_PART_DONE]

    def result_queue(self):
        tasks, results = self._queue_spawner()
        return results

    def _iter_queue(self):
        if self._limit is not None:
            allowed = self._limit[0]
        else:
            allowed = None
        finished = 0
        expected = self._num_query_parts()
        tasks, results = self._queue_spawner()
        while True:
            try:
                if finished < expected:
                    item = results.get()
                    if item is _QUERY_PART_DONE:
                        finished += 1
                    elif allowed is not None:
                        if allowed > 0:
                            allowed -= 1
                            yield item
                        else:
                            tasks.close()
                            break
                    else:
                        yield item
                else:
                    break
            # See if we can wait for another task to finish
            except DONE_EXCEPTIONS:
                if finished >= expected:  # Should never be greater than 
                    break                 # Just in case

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
            return self._iter_normal()
        else:
            return self._iter_queue()

    def new_with_parallel_config(self, queries, original_query=None):
        cls = type(self)
        if original_query is None:
            original_query = self.original_query
        return cls(queries, 
                   original_query,
                   backend=self._backend,
                   pool=self._pool,
                   spawn_context=self._spawn_contextmanager,
                   spawn_transformation=self._spawn_transformation)

    @property
    def session(self):
        return self.original_query.session

    def limit(size):
        new = self._apply_over_queries('limit', size)
        new._limit = (0, size)

    def _apply_over_queries(self, name, *args, **kwargs):
        apply_proxy = lambda q: getattr(q, name)(*args, **kwargs)
        original_query = apply_proxy(self.original_query)
        queries = []
        for query in self.queries:
            queries.append(apply_proxy(query))
        parallel_query = self.new_with_parallel_config(queries, original_query)
        return parallel_query

    def __getattr__(self, name):
        if not hasattr(self.original_query, name):
            raise AttributeError("{} not available in original query. Will not map over query parts".format(name))
        def proxy(*args, **kwargs):
            return self._apply_over_queries(name, *args, **kwargs)
        return proxy

    def __repr__(self):
        return '<ParallelQuery: {0} parts over "{1!r}">'.format(self._num_query_parts(), self.original_query)

