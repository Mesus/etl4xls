# -*- coding: utf-8 -*-
from sqlalchemy import Column, Integer, String, ForeignKey,DECIMAL
from sqlalchemy.orm import relationship, backref
from db_init import Base

class mcczb(Base):
    # 表的名字:
    __tablename__ = 'b2mcczb'
    __table_args__ = {'schema': 'mrcsb'}
    # 表的结构:
    id = Column(String(32),primary_key=True)
    parent_id = Column(String(32))
    pmdm = Column(String(100))
    insert_time	 = Column(String(100))
    farm = Column(String(100))
    sheet = Column(String(100))
    mcczl = Column(DECIMAL(50))
    dkh = Column(String(50))
    jgccsl = Column(DECIMAL(50))
    czr = Column(String(50))
    czsj = Column(String(50))
    scdd = Column(String(50))