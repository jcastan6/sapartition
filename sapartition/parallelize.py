import contextlib

# A backend is a tuple of a "pool" class and a "queue" class
# pool must have a imap_unordered method
# queue must have put and get methods
BACKENDS = {}
DEFAULT_BACKEND = None

DONE_EXCEPTIONS = (StopIteration,)

try:
    from multiprocessing.pool import ThreadPool
    from Queue import Queue as ThreadQueue
    from Queue import Empty as ThreadQueueEmpty
    DONE_EXCEPTIONS += (ThreadQueueEmpty,)
    THREADING_BACKEND = (ThreadPool, ThreadQueue)
    BACKENDS['threading'] = THREADING_BACKEND
    DEFAULT_BACKEND = 'threading'
except ImportError:
    THREADING_BACKEND = None

try:
    from gevent.pool import Pool as GeventPool
    from gevent.queue import Queue as GeventQueue
    from gevent.hub import LoopExit as GeventLoopExit
    DONE_EXCEPTIONS += (GeventLoopExit,)
    GEVENT_BACKEND = (GeventPool, GeventQueue)
    BACKENDS['gevent'] = GEVENT_BACKEND
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


class ParallelizableQueryMixin(object):
    def fork_with_transformations(self, transformations):
        return [query.with_transformation(transform) for transform in transformations]

    def parallelize_with_transformations(self, transformations, **kwargs):
        queries = self.fork_with_transformations(transformations)
        return ParallelizedQuery(queries, original=self, **kwargs)


class ParallelizedQuery(object):
    def __init__(self, queries, original_query, 
                 backend=DEFAULT_BACKEND, 
                 pool=None, 
                 spawn_context=no_context,
                 spawn_transformation=None):
        self.queries = queries
        self.original_query = original_query

        if backend is None:
            raise NotImplementedError("Cannot find a backend to parallelize with")

        self._backend = backend
        self._spawn_transformation = spawn_transformation
        self._spawn_contextmanager = spawn_context

        self._pool = pool

    def _spawnned_query_instance(self, query):
        if self._spawn_transformation is not None:
            query = query.with_transformation(self._spawn_transformation)
        query_context = self._spawn_contextmanager(query)
        return query_context

    def _spawn_callback_query_results(self, (input_query, callback)):
        with self._spawnned_query_instance(input_query) as query:
            for result in query:
                callback(result)
    
    def _spawn_load_all_results(self, input_query):
        with self._spawnned_query_instance(input_query) as query:
            results = query.all()
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
        print queueclass
        results = queueclass()
        tasks = self._spawn_with_callback(results.put)
        return tasks, results

    def all_per_query(self):
        tasks = self._spawner(self._spawn_load_all_results)
        return list(tasks)

    def all(self):
        per_query = self.all_per_query()
        return [result for results in per_query for result in results]

    def result_queue(self):
        tasks, results = self._queue_spawner()
        return results

    def _iter_queue(self):
        tasks, results = self._queue_spawner()
        for task in tasks:
            while True:
                try:
                    yield results.get()
                # See if we can wait for another task to finish
                except DONE_EXCEPTIONS:
                    pass

    def _iter_normal(self):
        tasks, results = self._queue_spawner()
        try:
            for result in results:
                yield result
        except DONE_EXCEPTIONS:
            pass

    def __iter__(self):
        if hasattr(self._backend[1], '__iter__'):
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

    def __getattr__(self, name):
        def proxy(*args, **kwargs):
            apply_proxy = lambda q: getattr(q, name)(*args, **kwargs)
            original_query = apply_proxy(self.original_query)
            queries = []
            for query in self.queries:
                queries.append(apply_proxy(query))
            return self.new_with_parallel_config(queries, original_query)
        return proxy

