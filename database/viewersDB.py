from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, desc
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import NoSuchTableError
import os


db_path = f"mysql+pymysql://root:Msirtz3173@localhost/yt_viewersdb"

engine = create_engine(db_path, echo=True)
Session = sessionmaker(bind=engine)
metadata = MetaData()


def create_table(table_name):
    try:
        table = Table(table_name, metadata,
            Column("sequence", Integer, primary_key=True),
            Column("time", String(50)),
            Column("viewers", Integer),
        )
        metadata.create_all(engine)
    except Exception as e:
        raise e


def get_latest_180_records(table_name):
    table = Table(table_name, metadata, autoload_with=engine)
    session = Session()

    try:
        query = table.select().order_by(desc(table.c.sequence)).limit(60)
        result = session.execute(query)
        rows = result.fetchall()

        viewer_dicts = []
        for row in rows:
            viewer_dict = row._mapping
            viewer_dicts.append(viewer_dict)

        # Sort the viewer_dicts in ascending order by sequence
        sorted_viewer_dicts = sorted(viewer_dicts, key=lambda x: x['sequence'], reverse=False)

        return sorted_viewer_dicts

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


def get_latest_8_records(table_name):
    table = Table(table_name, metadata, autoload_with=engine)
    session = Session()

    try:
        query = table.select().order_by(desc(table.c.sequence)).limit(8)
        result = session.execute(query)
        rows = result.fetchall()

        viewer_dicts = []
        for row in rows:
            viewer_dict = row._mapping
            viewer_dicts.append(viewer_dict)

        # Sort the viewer_dicts in ascending order by sequence
        sorted_viewer_dicts = sorted(viewer_dicts, key=lambda x: x['sequence'], reverse=False)

        return sorted_viewer_dicts

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


def delete_viewerTable(table_name):
    try:
        table = Table(table_name, metadata, autoload_with=engine)
        table.drop(engine)
        metadata.remove(table)

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
