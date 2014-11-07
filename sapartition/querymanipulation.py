from sqlalchemy.sql.visitors import replacement_traverse

def replace(old, new):
        replacer = PartitionReplacer(old.__table__).set_replacement(new.__table__)
        old_mapper = old.__mapper__
        new_mapper = new.__mapper__
        def transform(original):
                query = original.filter()
                query.__dict__.update({
                        '_criterion': replacement_traverse(query.__dict__['_criterion'], {}, replacer),
                        '_from_obj': tuple(replacement_traverse(fo, {}, replacer) for fo in query.__dict__['_from_obj']),
                        '_join_entities': tuple(new_mapper if ent is old_mapper else ent for ent in query.__dict__['_join_entities']),
                        '_joinpoint': {k: new if v is old else v for k,v in query.__dict__['_joinpoint'].items()},
                })
                return query
        return transform


def replace_table(old_table, new_table):
    replacer = PartitionReplacer(old_table, new_table)
    def transform(original):
        query = original.filter()  # Copy query

        from_obj = tuple(replacer.apply(obj) for obj in query._from_obj)
        query.__dict__['_from_obj'] = from_obj

        # Determine internal replacements
        if '_criterion' in query.__dict__:
            criterion = replacer.apply(query._criterion)
            query.__dict__['_criterion'] = criterion

        if '_order_by' in query.__dict__:
            order_by = tuple(replacer.apply(obj) for obj in query._order_by)
            query.__dict__['_order_by'] = order_by

        return query
    return transform


def replace_entity(old_cls, new_cls):
    old_table = old_cls.__table__
    old_map = old_cls.__mapper__
    new_table = new_cls.__table__
    new_map = new_cls.__mapper__

    def transform(original):
        query = original.with_transformation(replace_table(old_table, new_table))
        join_entities = tuple(new_map if ent is old_map else ent for ent in query._join_entities)
        joinpoint = dict((k, new_cls if v is old_cls else v) for k, v in query._joinpoint.items())

        # Apply replacements to internal query structure
        query.__dict__.update(_join_entities=join_entities,
                              _joinpoint=joinpoint)
        return query
    return transform


class PartitionReplacer(object):
    def __init__(self, search, replacement=None):
        self.search = search
        self.replacement = replacement

    def set_replacement(self, replacement):
        self.replacement = replacement
        return self

    def __call__(self, elem):
        # Replace instances of columns
        try:
            table = elem.table
            name = elem.name
            if table is self.search:
                replace = getattr(self.replacement.columns, name)
                return replace
        except AttributeError:
            pass
        # Replace instances of table
        if elem is self.search:
            return self.replacement
        return None

    def apply(self, target, options={}):
        return replacement_traverse(target, options, self)

