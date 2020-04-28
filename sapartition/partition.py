#!/usr/bin/env python

# Copyright (C) 2014 Teague Sterling, Regents of the University of California 

# From http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PartitionTable
from collections import OrderedDict
import itertools
import sys
from pprint import pprint

from sqlalchemy.schema import Table, Column, CreateTable
from sqlalchemy.sql.expression import Alias
from sqlalchemy.ext.compiler import compiles

from sqlalchemy import *
from sqlalchemy.schema import CreateTable
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.declarative import *


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
                        '_joinpoint': {k: new if v is old else v for k,v in list(query.__dict__['_joinpoint'].items())},
                })
                return query
        return transform


def replace_table(old_table, new_table):
    replacer = PartitionReplacer(old_table, new_table)
    def transform(original):
        query = original.filter()  # Copy query

        # Determine internal replacements
        criterion = replacer.apply(query._criterion)
        from_obj = tuple(replacer.apply(obj) for obj in query._from_obj)

        # Apply replacements to internal query structure
        query.__dict__.update(_criterion=criterion,
                              _from_obj=from_obj)

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
        joinpoint = dict((k, new_cls if v is old_cls else v) for k, v in list(query._joinpoint.items()))

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


def _find_subelement_replacements(element, tables_with_new=True, parameters_with_values=True):
    names = set()
    if hasattr(element, 'params') and parameters_with_values:
        parameters_with_values = False
        for name, value in list(element.compile().params.items()):
            replacement = (":{}".format(name), " {!r} ".format(value))
            names.add(replacement)
        if len(names) == 0:
            parameters_with_values = True  # No parameters found

    if hasattr(element, 'clauses'):
        for clause in element.clauses:
            names.update(_find_subelement_replacements(clause, tables_with_new, parameters_with_values))
    if hasattr(element, 'left'):
        names.update(_find_subelement_replacements(element.left, tables_with_new, parameters_with_values))
    if hasattr(element, 'right'):
        names.update(_find_subelement_replacements(element.right, tables_with_new, parameters_with_values))

    if hasattr(element, 'table') and tables_with_new:
        old = str(element.compile())
        new = "NEW.{}".format(element.name)
        names.add((old, new))
    return names


class Partition(object):
    """Represents a 'table partition'."""

    def mock_parent(self):
        return PartitionAlias(self, __parenttable__.fullname)


class PartitionAlias(Alias):

    def alias(self, name):
        a = Alias(self, name)
        a.original = self
        return a


@compiles(CreateTable, 'postgresql')
def create_partition_table(create, compiler, **kwargs):
    parent_table = getattr(create.element, '__parenttable__', None)
    create_ddl = compiler.visit_create_table(create, **kwargs)
    if parent_table is not None:
        parent_name = parent_table.fullname
        create_ddl = "{create} INHERITS ({parent})".format(create=create_ddl,
                                                          parent=parent_name)
    return create_ddl
    
        
@compiles(PartitionAlias)
def visit_partition(element, compiler, **kw):
    if kw.get('asfrom'):
        return element.name
    else:
        return compiler.visit_alias(element)


def copy_model(source, new_table, new_class=None, new_bases=None):
    if new_class is None:
        new_class = new_table.capitalize()
    if new_bases is None:
        new_bases = source.__bases__

    definition = []
    definition.extend((col.name, col.copy()) for col in source.__table__.columns)
    definition.append(('__tablename__', new_table))

    copied_model = type(new_class, new_bases, dict(definition))
    return copied_model


class Partitioned(object):
    __partitionprefix__ = None
    __partitioncolumn__ = None
    __partitionconstraint__ = None

    __generate_partitions__ = None
    __partitions_loaded__ = False
    __partitions__ = None

    @classmethod
    def get_partition(cls, partition_name, constraint=None, **definition):
        try:
            return cls.__partitions__[partition_name]
        except KeyError:
            return cls.define_partition(partition_name, constraint, **definition)

    @classmethod
    def define_partition(cls, partition_name, constraint=None, **definition):
        if cls.__partitions__ is None:
            cls.__partitions__ = OrderedDict()

        if partition_name is cls.__partitions__:
            raise ValueError("Cannot redefine partition: {}".format(partition_name))

        if cls.__partitionprefix__ is not None:
            prefix = cls.__partitionprefix__
        elif cls.__partitioncolumn__ is not None:
            prefix_column = cls.__partitioncolumn__
            if callable(prefix_column):
                prefix_column = prefix_column()
            prefix = "{0}_{1}_".format(cls.__tablename__,
                                       prefix_column.name)
        else:
            prefix = cls.__tablename__ + "_"

        partition_table = prefix + partition_name
        partition_cls = partition_table.capitalize()

        # Make sure we don't attempt to redefine the parititon in metadata
        if partition_table in cls.metadata.tables:
            return cls._decl_class_registry[partition_cls]

        bases = tuple(base for base in cls.__bases__ if base is not Partitioned) + (Partition,)

        definition.update({
            '__tablename__': partition_table,
            '__parenttable__': cls.__table__,
        })

        # Defer
        definition['__raw_partition_constraint__'] = constraint

        partition = type(partition_cls, bases, definition)
        cls.__partitions__[partition_name] = partition

        if partition.__raw_partition_constraint__ is not None:
            raw_constraint = partition.__raw_partition_constraint__
            if hasattr(raw_constraint, '__call__'):
                try:
                    raw_constraint = raw_constraint(partition)
                except TypeError:
                    raw_constraint = raw_constraint.__func__(partition)  # Force raw call

            constraint_name = "cst_" + partition_table + "_partition"
            constraint_clause = CheckConstraint(raw_constraint, name=constraint_name)
        else:
            constraint_clause = None
        setattr(partition, '__partitionconstraint__', constraint_clause)
        setattr(partition.__table__, '__parenttable__', partition.__parenttable__)

        return partition

    @classmethod
    def partition_defined(cls, name):
        if cls.__partitions__ is None:
            cls.__partitions__ = OrderedDict()

        return name in cls.__partitions__

    @classmethod
    def load_partitions(cls):
        if getattr(cls, '__partitions__', None) is None:
            cls.__partitions__ = OrderedDict()

        if getattr(cls, '__generate_partitions__', None) is not None and not cls.__partitions_loaded__:
            cls.__generate_partitions__()
            cls.__partitions_loaded__ = True

    @classmethod
    def partitions(cls):
        cls.load_partitions()
        return list(cls.__partitions__.values())

    # TODO: Replace with custom DDL hooks
    @classmethod
    def create_insert_trigger_ddl(cls):
        cls.load_partitions()
        if all(part.__partitionconstraint__ is None for part in list(cls.__partitions__.values())):
            return None
        
        parent_table = cls.__tablename__
        function_name = parent_table + "_insert_function"
        trigger_name = parent_table + "_insert_trigger"

        trigger_start = """
            CREATE OR REPLACE FUNCTION {fn}()
            RETURNS TRIGGER AS $$
                BEGIN

        """.format(fn=function_name)
        trigger_checks = []
        first = True
        for partition in list(cls.__partitions__.values()):
            if partition.__partitionconstraint__ is None:
                continue
            if first:
                check_tpl = "IF ({test}) THEN"
                first = False
            else:
                check_tpl = "ELSIF ({test}) THEN"
            check_tpl += """
                INSERT INTO {partition_name} VALUES (NEW.*);
            """
            test_structure = partition.__partitionconstraint__.sqltext
            replacements = sorted(_find_subelement_replacements(test_structure,
                                                                tables_with_new=True,  # Cooerse table to NEW row
                                                                parameters_with_values=True), key=len)  # Hardcode parameters
            test = str(test_structure)
            for old, new in replacements:
                test = test.replace(old, new)
            check = check_tpl.format(partition_name=partition.__tablename__,
                                     test=test)
            trigger_checks.append(check)

        trigger_end = """
            ELSE
                RAISE EXCEPTION 'Insert error on {parent}. No child defined. Consider updating {fn}()';
            END IF;
            RETURN NULL;
            END;
            $$
            LANGUAGE plpgsql;

            CREATE TRIGGER {trigger}
            BEFORE INSERT ON {parent}
            FOR EACH ROW EXECUTE PROCEDURE {fn};
        """.format(parent=parent_table,
                   trigger=trigger_name,
                   fn=function_name)

        sql = trigger_start + "\n".join(trigger_checks) + trigger_end
        return sql


def test():
    
    metadata = MetaData()
#    t1 = Table('sometable', metadata,
#        Column('id', Integer, primary_key=True),
#        Column('data', String(50))
#    )
#
#    print select([t1]).where(t1.c.data == 'foo')
#
#    print
#    
#    t1_partition_a = Partition(t1, "partition_a")
#    print select([t1_partition_a]).where(t1_partition_a.c.data=='foo')
#    
#    print
#    
#    t1_p_alias = t1_partition_a.alias()
#    print select([t1_p_alias]).where(t1_p_alias.c.data=='foo')
#
#    print "-" * 80
#
    Base = declarative_base()

    class FingerprintId(Base):
        __tablename__ = 'fpid'
        id = Column('fpid', Integer, primary_key=True)
        substances = relationship('Substance', backref="fingerprints")

    class Substance(Base):
        __tablename__ = 'substance'
        id = Column('sub_id', Integer, primary_key=True)
        smiles = Column('smiles', String)
        fingerprint = Column('fp', ForeignKey(FingerprintId.id))

    class FingerprintTable(object):
        __partitioncolumn__ = classmethod(lambda cls: cls.id)

        @declared_attr
        def id(cls): 
            return Column('fp_id', ForeignKey('fpid.fpid'), primary_key=True)

        @declared_attr
        def ecfp4(cls):
            return Column('ecfp4_fp', String)

        @declared_attr
        def substances(cls):
            return relationship(Substance, 
                                secondary="fpid",
                                primaryjoin="{}.id==FingerprintId.id".format(cls.__name__),
                                secondaryjoin=FingerprintId.id==Substance.fingerprint)


    class Fingerprint(Base, FingerprintTable, Partitioned):
        __tablename__ = 'fingerprints'
        
        test_parts = [(1, 500000), (500001, 1000000), (1000001, 150000)]

        @classmethod
        def __generate_partitions__(cls):
            for low, high in cls.test_parts:
                name = "{0}_{1}".format(low, high)
                check = lambda cls: (cls.id >= low) & (cls.id < high)
                part = cls.define_partition(name, constraint=check, ID_LOWER_BOUND=low, ID_UPPER_BOUND=high)

        
    for part in Fingerprint.partitions():
        print(part.get_create_ddl())

    print()
    session = Session()

    for partition in Fingerprint.partitions():
        q = session.query(Substance)\
                   .join(FingerprintId)\
                   .join(partition)\
                   .filter(partition.ecfp4 % 'bla')
        print(q)
        print()

    print(Fingerprint.create_insert_trigger_ddl())

    return Substance, FingerprintId, Fingerprint

if __name__ == '__main__':
    test()
