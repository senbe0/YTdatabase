from fastapi import FastAPI
import hashlib

from .database import videosDB
from .database import viewersDB


app = FastAPI()

def convert_video_id_to_table_name(videoID):
    hash_object = hashlib.md5(videoID.encode('utf-8'))
    hex_dig = hash_object.hexdigest()
    # Add `v` at the beginning to make it a table name To avoid table name errors.
    table_name = "v" + hex_dig
    return table_name



@app.post("/videosDB/insert")
async def insert_record_to_videosDB(record: dict):
    videoID = record["videoID"]
    channelID = record["channelID"]
    title = record["title"]
    videoURL = record["videoURL"]
    iconImageURL = record["iconImageURL"]
    try:
        record = videosDB.get_video_record_by_id(videoID)
        if record:
            if record.title != title:
                videosDB.update_video_title(videoID, title)
                return {"status": "update"}
            return {"status": "already"}
        else:
            videosDB.insert_videoRecord(videoID, channelID, title, videoURL, iconImageURL)
            return {"status": "success"}
    except Exception as e:
        return {"status": "failure", "msg": e}



@app.post("/videosDB/delete")
async def delete_record_from_videoDB(info: dict):
    videoID = info["videoID"]
    try:
        videosDB.delete__videoRecord(videoID)
        return {"status": "success"}
    except Exception as e:
        return {"status": "failure", "msg": e}



@app.post("/viewersDB/createTable")
async def create_table_into_viewersDB(info: dict):
    tableName = info["tableName"]
    try:
        viewersDB.create_table(tableName)
        return {"status": "success"}
    except Exception as e:
        return {"status": "failure", "msg": e}

@app.post("/viewersDB/delete")
async def delete_table_from_viewersDB(info: dict):
    tableName = info["tableName"]
    try:
        viewersDB.delete_viewerTable(tableName)
        return {"status": "success"}
    except Exception as e:
        return {"status": "failure", "msg": e}

@app.post("/viewersDB/insert")
async def insert_record_into_viewersDB(record: dict):
    tableName = record["tableName"]
    time = record["time"]
    viewers = record["viewers"]
    try:
        viewersDB.insert_viewerRecord(tableName, time, viewers)
        return {"status": "success"}
    except Exception as e:
        return {"status": "failure", "msg": e}


@app.get("/getVideoObjList_all")
async def get_video_objects_List():
    video_obj_list = []
    videosList = videosDB.select_all_from_videosTable()
    for videoInfo in videosList:
        videoInfo_dict = videoInfo.__dict__
        videoID = videoInfo_dict["videoID"]
        tableName = convert_video_id_to_table_name(videoID)
        try:
            viewersRecordsList = viewersDB.get_latest_180_records(tableName)
            if len(viewersRecordsList) != 0:
                video_object = {
                    "videoID": videoInfo_dict["videoID"],
                    "channelID": videoInfo_dict["channelID"],
                    "videoURL": videoInfo_dict["videoURL"],
                    "videoTitle": videoInfo_dict["title"],
                    # *viewersData exsample*
                    # "viewersData": [{sequence: 1, time: "20:30", viewers: 1900}, {sequence: 2, time: 20:21, viewers: 1200} ...,
                    "viewersData": viewersRecordsList,
                    "iconURL": videoInfo_dict["IconImageURL"]
                }

                video_obj_list.append(video_object)

        except Exception as e:
            pass

    return {"video_obj_list": video_obj_list}


@app.get("/getVideoObjList")
async def get_video_objects_List(lang: str = None):
    print(lang)
    video_obj_list = []
    videosList = videosDB.get_video_records_by_group_name(lang)
    for videoInfo in videosList:
        videoInfo_dict = videoInfo.__dict__
        videoID = videoInfo_dict["videoID"]
        tableName = convert_video_id_to_table_name(videoID)
        try:
            viewersRecordsList = viewersDB.get_latest_180_records(tableName)
            if len(viewersRecordsList) != 0:
                video_object = {
                    "videoID": videoInfo_dict["videoID"],
                    "channelID": videoInfo_dict["channelID"],
                    "videoURL": videoInfo_dict["videoURL"],
                    "videoTitle": videoInfo_dict["title"],
                    # *viewersData exsample*
                    # "viewersData": [{sequence: 1, time: "20:30", viewers: 1900}, {sequence: 2, time: 20:21, viewers: 1200} ...,
                    "viewersData": viewersRecordsList,
                    "iconURL": videoInfo_dict["IconImageURL"]
                }

                video_obj_list.append(video_object)

        except Exception as e:
            pass

    return {"video_obj_list": video_obj_list}

