from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from sqlalchemy import create_engine, Column, String, Integer

engine = create_engine('mysql+pymysql://root:qwer123456@127.0.0.1:3306/mediamz_sim?charset=utf8mb4')
Base = declarative_base(engine)
session = sessionmaker(engine)()


def conn_test():
    conn = engine.connect()
    result = conn.execute('select * from tbl_country')
    print(result.fetchall())
    conn.close()


def orm_test():
    item_list = session.query(Operation.id, Operation.op_content).filter(Operation.id > 20, Operation.id < 25).order_by(
        Operation.id.desc()).all()
    print(item_list)
    session.close()


class Operation(Base):
    __tablename__ = 'tbl_operation_log'
    id = Column(Integer, primary_key=True, nullable=False, autoincrement=True)
    op_user = Column(String(255))
    op_user_id = Column(Integer)
    op_content = Column(String(255))


Base.metadata.create_all()

if __name__ == '__main__':
    orm_test()
