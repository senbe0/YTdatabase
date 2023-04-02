from fastapi import FastAPI
from fastapi import Depends
import json

from .database import videosDB
from .database import viewersDB


app = FastAPI()


@app.post("/videosDB/insert")
async def insert_record_to_videosDB(record: dict):
    videoID = record["videoID"]
    channelID = record["channelID"]
    title = record["title"]
    videoURL = record["videoURL"]
    iconImageURL = record["iconImageURL"]
    try:
        videosDB.insert_videoRecord(videoID, channelID, title, videoURL, iconImageURL)
        return {"status": "success"}
    except:
        return {"status": "failure"}

@app.post("/videosDB/delete")
async def delete_record_from_videoDB(info: dict):
    videoID = info["videoID"]
    try:
        videosDB.delete__videoRecord(videoID)
        return {"status": "success"}
    except:
        return {"status": "failure"}



@app.post("/viewersDB/createTable")
async def create_table_into_viewersDB(info: dict):
    tableName = info["tableName"]
    try:
        viewersDB.create_table(tableName)
        return {"status": "success"}
    except:
        return {"status": "failure"}

@app.post("/viewersDB/delete")
async def delete_table_from_viewersDB(info: dict):
    tableName = info["tableName"]
    try:
        viewersDB.delete_viewerTable(tableName)
        return {"status": "success"}
    except:
        return {"status": "failure"}

@app.post("/viewersDB/insert")
async def insert_record_into_viewersDB(record: dict):
    tableName = record["tableName"]
    time = record["time"]
    viewers = record["viewers"]
    try:
        viewersDB.insert_viewerRecord(tableName, time, viewers)
        return {"status": "success"}
    except:
        return {"status": "failure"}
