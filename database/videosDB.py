from sqlalchemy import create_engine, Column, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import os

 
db_path = f"mysql+pymysql://root:Msirtz3173@localhost/yt_videosdb"

engine = create_engine(db_path, echo=True)
Session = sessionmaker(bind=engine)
Base = declarative_base()


class VideoRecord(Base):
    __tablename__ = "video_records"

    __mapper_args__ = {
        "exclude_properties": ["__dict__"]
    }

    videoID = Column(String(255), primary_key=True)
    channelID = Column(String(255))
    title = Column(String(255))
    videoURL = Column(String(255))
    IconImageURL = Column(String(255))

class Category(Base):
    __tablename__ = "video_category"

    __mapper_args__ = {
        "exclude_properties": ["__dict__"]
    }

    channelID = Column(String(255), primary_key=True)
    group_name = Column(String(255))



def create_table():
    Base.metadata.create_all(engine)



def get_video_record_by_id(video_id: str):
    session = Session()
    try:
        record = session.query(VideoRecord).filter_by(videoID=video_id).first()
        return record
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def update_video_title(video_id: str, new_title: str):
    session = Session()
    try:
        video = session.query(VideoRecord).filter_by(videoID=video_id).first()
        if video:
            video.title = new_title
            session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def select_all_from_videosTable():
    session = Session()
    try:
        records = session.query(VideoRecord).all()
        return records
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def get_video_records_by_group_name(group_name: str):
    session = Session()
    try:
        records = session.query(VideoRecord).join(Category, VideoRecord.channelID == Category.channelID).filter(Category.group_name == group_name).all()
        return records
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def insert_videoRecord(db_videoID, db_channelID, db_title, db_videoURL, db_iconImageURL):
    session = Session()

    try:
        video_record = VideoRecord(
            videoID=db_videoID,
            channelID=db_channelID,
            title=db_title,
            videoURL=db_videoURL,
            IconImageURL=db_iconImageURL)
        session.add(video_record)
        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


def delete__videoRecord(db_videoID: str):
    session = Session()

    try:
        video =session.query(VideoRecord).filter_by(videoID=db_videoID).first()
        session.delete(video)
        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()


def update_iconImageURL(videoID: str, iconImageURL: str):
    session = Session()

    try:
        video = session.query(VideoRecord).filter_by(videoID=videoID).first()
        video.IconImageURL = iconImageURL
        session.commit()

    except Exception as e:
        session.rollback()
        raise e

    finally:
        session.close()



if __name__ == "__main__":
    # if NOT exist table, create it.
    create_table()