# -*- coding: utf-8 -*-
# 导入:
from sqlalchemy import Column, String, create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import psycopg2

# 创建对象的基类:
Base = declarative_base()

# 初始化数据库连接:
engine = create_engine('postgresql+psycopg2://postgres:bXlZcWw2@10.0.100.128/postgres',echo = True)
# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)
dbsession = DBSession()