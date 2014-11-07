""":mod:`future` --- SQLAlchemy-Future
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
SQLAlchemy-Future is a SQLAlchemy_ extension that introduces `future/promise`_
into query.
.. _SQLAlchemy: http://www.sqlalchemy.org/
.. _future/promise: http://en.wikipedia.org/wiki/Futures_and_promises
How to setup
============
In order to make :class:`future.Query <Query>` the default query class, use
the ``query_cls`` parameter of the
:func:`~sqlalchemy.orm.session.sessionmaker()` function::
    import future
    from sqlalchemy.orm.session import sessionmaker
    Session = sessionmaker(query_cls=future.Query)
Or you can make :class:`future.Query <Query>` the query class of a session
instance optionally::
    session = Session(query_cls=future.Query)
How to promise
==============
How to promise a future query is not hard. Just call the
:meth:`~Query.promise()` method::
    posts = session.query(Post).promise()
Its return type is :class:`Future` (note that it is not a subtype of
:class:`Query`, so you cannot use rich facilities of :class:`Query` like
:meth:`~sqlalchemy.orm.query.Query.filter`)::
    assert isinstance(posts, future.Future)
Then, iterate this future query (``posts`` in the example) when you really
need it::
    for post in posts:
        print post.title, "by", post.author
If the ``posts`` finished the execution in the another thread, it fetches the
result set immediately. If its execution hasn't finished, the another thread
joins the main thread and it has blocked until its execution has finished.
References
==========
"""
from collections import deque

USE_GREEN_THREADS = True
try:
    if not USE_GREEN_THREADS:
        raise ImportError("Manual fallback to threading")
    import gevent
    spawner = gevent.Greenlet.spawn
    joiner = gevent.Greenlet.join
except ImportError:
    import threading
    joiner = threading.Thread.join
    def spawner(fn):
        thread = threading.Thread(target=fn)
        thread.start()
        return thread
        
import sqlalchemy.orm.query


class FutureMixin(object):
    def promise(self, *args, **kwargs):
        """Makes a promise and returns a :class:`Future`.
        :returns: the promised future
        :rtype: :class:`Future`
        """
        return Future(self, *args, **kwargs)

    def promise_with_transformation(self, transformation, *args, **kwargs):
        """Makes a promise that will apply a transformation within
           the new thread and returns a :class:`Future`.
        :returns: the promised future
        :rtype: :class:`Future`
        """
        return Future(self, transformation, *args, **kwargs)


class FutureQuery(FutureMixin, sqlalchemy.orm.query.Query):
    """The subtype of :class:`sqlalchemy.orm.query.Query` class, that provides
    the :meth:`promise()` method.
    You can make this the default query class of your session::
        from sqlalchemy.orm import sessionmaker
        import future
        Session = sessionmaker(query_cls=future.FutureQuery)
    """
    pass


EMPTY = object()


class Future(object):
    """Promoised future query result.
    :param query: the query to promise
    :type query: :class:`sqlalchemy.orm.query.Query`
    .. note::
    
       It is not a subtype of :class:`Query`, so it does not provide any
       method of :class:`Query` like :meth:`~Query.filter()`.
    """

    __slots__ = "query", "_iter", "_buffer", "_thread", "_transform", '_callback', '_join'

    def __init__(self, query, transform=None, callback=None, load_all=True, spawn=spawner, join=joiner):
        self.query = query
        self._iter = None
        self._buffer = EMPTY
        self._transform = transform
        self._callback = callback
        if load_all:
            target = self.execute_promise_all
        else:
            target = self.execute_promise
        self._thread = spawn(target)
        self._join = joiner

    def execute_promise(self):
        if self._transform is not None:
            self.query = self.query.with_transformation(self._transform)

        self._iter = iter(self.query)
        try:
            self._buffer = deque([next(self._iter)])
        except StopIteration:
            self._buffer = EMPTY

    def execute_promise_all(self):
        self.execute_promise()
        for value in self._iter:
            self._buffer.append(value)
        self._done()

    def _done(self):
        if self._iter is not None:
            self._iter = None
            if self._callback is not None:
                self._callback(self, self.query)

    def __iter__(self):
        if self._iter is None or self._buffer is EMPTY:
            self._join(self._thread)
        def gen():
            if self._buffer is not EMPTY:
                try:
                    while True:
                        yield self._buffer.popleft()
                except IndexError:
                    pass
            if self._iter is not None:
                for value in self._iter:
                    yield value
            self._done()
        return gen()

    def all(self):
        """Returns the results promised as a :class:`list`. This blocks the
        underlying execution thread until the execution has finished if it
        is not yet.
        :returns: the results promised
        :rtype: :class:`list`
        """
        return list(self)
