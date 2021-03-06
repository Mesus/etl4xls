# -*- coding: utf-8 -*-
from sqlalchemy import Column, Integer, String, ForeignKey,DECIMAL
from sqlalchemy.orm import relationship, backref
from db_init import Base

class mrcsb(Base):
    # 表的名字:
    __tablename__ = 'b2mrcsb'
    __table_args__ = {'schema': 'mrcsb'}
    # 表的结构:
    id = Column(String(32),primary_key=True)
    parent_id = Column(String(32))
    pmdm = Column(String(100))
    insert_time	 = Column(String(100))
    farm = Column(String(100))
    sheet = Column(String(100))
    jpcjgsl = Column(DECIMAL(50))
    spcjgsl = Column(DECIMAL(50))
    jpczkc = Column(DECIMAL(50))
    jpccksl = Column(DECIMAL(50))
    spccksl = Column(DECIMAL(50))
    spczk = Column(DECIMAL(50))
    jpcjg_1 = Column(DECIMAL(50))
    jpcjg_2 = Column(DECIMAL(50))
    jpcjg_3 = Column(DECIMAL(50))
    spcjg_ccsl = Column(DECIMAL(50))
    jpczkc_1 = Column(DECIMAL(50))
    jpczkc_2 = Column(DECIMAL(50))
    jpczkc_3 = Column(DECIMAL(50))
    jpczkc_4 = Column(DECIMAL(50))
    jpcdbrk_1 = Column(DECIMAL(50))
    jpcdbrk_2 = Column(DECIMAL(50))
    spczkc_1 = Column(DECIMAL(50))
    spczkc_2 = Column(DECIMAL(50))
    spczkc_3 = Column(DECIMAL(50))
    spczkc_4 = Column(DECIMAL(50))
    spcdbrk_1 = Column(DECIMAL(50))
    spcdbrk_2 = Column(DECIMAL(50))
    jpcck_1 = Column(DECIMAL(50))
    jpcck_2 = Column(DECIMAL(50))
    jpcck_3 = Column(DECIMAL(50))
    jpctgck_1 = Column(DECIMAL(50))
    jpctgck_2 = Column(DECIMAL(50))
    jpctgck_3 = Column(DECIMAL(50))
    jpctgck_4 = Column(DECIMAL(50))
    jpctgck_5 = Column(DECIMAL(50))
    jpctgck_6 = Column(DECIMAL(50))
    jpcdbck_1 = Column(DECIMAL(50))
    jpcdbck_2 = Column(DECIMAL(50))
    spcck_1 = Column(DECIMAL(50))
    spcck_2 = Column(DECIMAL(50))
    spcdbck_1 = Column(DECIMAL(50))
    spcdbck_2 = Column(DECIMAL(50))
    dbdd = Column(DECIMAL(50))
    xsdd = Column(DECIMAL(50))
    drspcck_1 = Column(DECIMAL(50))
