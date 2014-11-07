from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.orm import scoped_session

sessionmaker_ = sessionmaker


def pooled_sessionmaker(pool, sessionmaker=sessionmaker_):
    def factory(*args,**kwargs):
        connection = pool.connect()
        session = sessionmaker(bind=connection)
        return session
    return factory


def use_new_scoped_session(sessionmaker=sessionmaker_):
    def transform(original):
        Session = scoped_session(sessionmaker())
        query = original.with_session(Session())
        assert original.session is not query.session
        return query
    return transform


def close_on_finish(future, query):
    query.session.connection().close()


def parallelize_with_transformations(transformers, sessionmaker, query, **kwargs):
    apply_scoped_session = use_new_scoped_session(sessionmaker)
    for inner_transformer in transformers:
        def make_promise_transformer(transformer):
            def promise_transformer(original):
                parallel = original
                parallel = parallel.with_transformation(apply_scoped_session)
                parallel = parallel.with_transformation(transformer)
                return parallel
            return promise_transformer
        outer_transformer = make_promise_transformer(inner_transformer)
        yield query.promise_with_transformation(outer_transformer, **kwargs)
