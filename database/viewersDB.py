from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoSuchTableError
import os


db_path = f"mysql+pymysql://root:Msirtz3173@localhost/yt_viewersdb"

engine = create_engine(db_path, echo=True)
Session = sessionmaker(bind=engine)
metadata = MetaData()


def create_table(table_name):
    table = Table(table_name, metadata,
        Column("sequence", Integer, primary_key=True),
        Column("time", String),
        Column("viewers", Integer),
    )

    metadata.create_all(engine)

def select_all_from_viewersTable(table_name):
    table = Table(table_name, metadata, autoload_with=engine)
    session = Session()

    try:
        query = table.select()
        result = session.execute(query)
        rows = result.fetchall()

        viewer_dicts = []
        for row in rows:
            viewer_dict = row._mapping
            viewer_dicts.append(viewer_dict)
        return viewer_dicts


    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


def delete_viewerTable(table_name):
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        table.drop(engine)

    except NoSuchTableError:
        pass

def insert_viewerRecord(table_name, db_time: str, db_viewers: int):
    table = Table(table_name, metadata, autoload_with=engine)

    session = Session()

    try:
        ins = table.insert().values(time=db_time, viewers=db_viewers)
        session.execute(ins)

        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()
