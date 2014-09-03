#!/usr/bin/env python

# Copyright (C) 2014 Teague Sterling, Regents of the University of California 

# From http://www.sqlalchemy.org/trac/wiki/UsageRecipes/PartitionTable
import sys
import itertools
from pprint import pprint

from sqlalchemy.schema import Table, Column
from sqlalchemy.sql.expression import Alias
from sqlalchemy.ext.compiler import compiles

from sqlalchemy import *
from sqlalchemy.orm import relationship, backref
from sqlalchemy.orm.session import Session
from sqlalchemy.ext.declarative import *

class Partition(Alias):
    """Represents a 'table partition'."""
    
    def alias(self, name=None):
        """Allow alias() of the partition to take place."""
        
        a = Alias(self, name)
        a.original = self
        return a

        
@compiles(Partition)
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
    __partitionprefix__ = ""

    @classmethod
    def partition(cls, partition_name, **definition):
        partition_table = cls.__partitionprefix__ + partition_name

        # Make sure we don't attempt to redefine the parititon in metadata
        if partition_table in cls.metadata.tables:
            return cls.metadata.tables[partition_table]

        partition_cls = partition_table.capitalize()
        bases = tuple(base for base in cls.__bases__ if base is not Partitioned)

        definition.update({
            '__tablename__': partition_table,
            '__parenttable__': cls.__table__,
        })

        partition = type(partition_cls, bases, definition)
#        aliasable_partition = Partition(partition)
        aliasable_partition = partition

        return aliasable_partition

def test():
    
    metadata = MetaData()
    t1 = Table('sometable', metadata,
        Column('id', Integer, primary_key=True),
        Column('data', String(50))
    )

    print select([t1]).where(t1.c.data == 'foo')

    print
    
    t1_partition_a = Partition(t1, "partition_a")
    print select([t1_partition_a]).where(t1_partition_a.c.data=='foo')
    
    print
    
    t1_p_alias = t1_partition_a.alias()
    print select([t1_p_alias]).where(t1_p_alias.c.data=='foo')

    print "-" * 80

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
        __partitionprefix__ = __tablename__ + "_path_bits_"
        __partitionbase__ = FingerprintTable

        __partitions__ = [(500, 510), (510, 520), (520, 530), (530, 540)]

        @classmethod
        def partitions_in_range(cls, range):
            l, h = range
            used = ["_".join(map(str, p)) for p in cls.__partitions__ if p[0] <= h and l < p[1]]
            return [cls.partition(p) for p in used]

        @classmethod
        def partitions(cls):
            partitions = ["_".join(map(str, p)) for p in cls.__partitions__]
            return [cls.partition(p) for p in partitions]
                    

    print
    session = Session()

    for partition in Fingerprint.partitions_in_range((511, 525)):
        q = session.query(Substance)\
                   .join(FingerprintId)\
                   .join(partition)\
                   .filter(partition.ecfp4 % 'bla')
        print q
        print

    return Substance, FingerprintId, Fingerprint

if __name__ == '__main__':
    test()
