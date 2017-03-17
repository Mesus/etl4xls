# -*- coding: utf-8 -*-
from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship, backref
from db_init import Base

class originaldata(Base):
    # 表的名字:
    __tablename__ = 'originaldata'
    __table_args__ = {'schema': 'mrcsb'}
    # 表的结构:
    id = Column(String(32),primary_key=True)
    farm = Column(String(10))
    filename = Column(String(50))
    insert_time = Column(String(50))