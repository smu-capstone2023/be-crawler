# Python 3.8.10 - 64-bit

# pip3 install beautifulsoup4
# pip3 install requests
# pip3 install pymongo
# pip3 install schedule
# pip3 install google-cloud-vision
# pip3 install python-dotenv
# pip3 install pytz

# DB - database name: smus
# DB - collection name: bus_notice
# DB - OCR collection name: ocr_history

import os
from dotenv import load_dotenv
import hashlib # calc MD5

# crawling
import requests
import urllib3
from bs4 import BeautifulSoup as bs

# parser
from urllib import parse #url parser
import xml.etree.ElementTree as et #xml parser
import json # json parser
from datetime import datetime
import time
import pytz # time parser

from pymongo import MongoClient
import schedule

# ssl 연결 경고 무시하기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
load_dotenv()

BUS_LIST = [
    {"busName": "7016", "busId": "100100447"},
    {"busName": "1711", "busId": "100100185"},
    {"busName": "163", "busId": "100100032"},
    {"busName": "서대문08", "busId": "100900012"},
    {"busName": "종로13", "busId": "100900002"}
]

def convertTime(timeString):
    return datetime.strptime(timeString, "%Y-%m-%d %H:%M:%S")

# 슬랙으로 메시지 보내기
def sendMessageToSlack(message):
    SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
    SLACK_ERROR_CHANNEL = os.getenv('SLACK_ERROR_CHANNEL')

    requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer " + SLACK_BOT_TOKEN},
        data={"channel": SLACK_ERROR_CHANNEL,"text": message})

# 일반 오류 메시지
def getNormalErrorMessage(error):
    issueData = {
        "error": str(error)
    }
    errorMessage = {
        "File": "Bus_Route_Crawling", 
        "Level": "error",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "Message": "버스 우회 크롤러에서 예상치 못한 에라가 발생해 이번 크롤링이 실패했습니다. ",
        "data": issueData
    }
    return json.dumps(errorMessage, ensure_ascii = False, indent=2)

def getMD5(value):
    return hashlib.md5(str(value).encode()).hexdigest()

# 특정 노선 모든 정류장 도착 정보 api 받기
def requestBusStopsApiByRoute(busId):
    baseURL = f"http://ws.bus.go.kr/api/rest/arrive/getArrInfoByRouteAll?"
    baseQuery = {}
    baseQuery["serviceKey"] = os.getenv('OPEN_API_KEY')
    baseQuery["busRouteId"] = busId
    encodingParse = parse.urlencode(baseQuery, doseq=True)
    requestURL = baseURL + encodingParse
    return requests.get(requestURL, verify=False).content

# 해당 노선 전체 정류장 중 우회한 정류장 리스트 반환
def parseBusStopXml(xmlString):
    parsedXml = et.fromstring(xmlString)
    busStops = parsedXml.iter(tag="itemList")
    bypassStops = []
    for stop in busStops:
        # 우회 여부 -> deTourAt: 00==정상;11==우회
        if stop.find("deTourAt").text == "11":
            bypassStop = {}
            # 정류장 이름 -> "stNm"
            bypassStop["stopName"] = stop.find("stNm").text
            # 정류장 id -> "arsId"
            bypassStop["stopId"] = stop.find("arsId").text
            # 방향 -> ???
            
            bypassStops.append(bypassStop)
    return bypassStops

# 버스 고유id로 우회 정류장 리스트 반환
def getBusBypassStops(busId, busName):
    xmlString = requestBusStopsApiByRoute(busId)
    bypassStops = parseBusStopXml(xmlString)
    return {
        "busName": busName,
        "busId": busId,
        "stops": bypassStops,
        "stopsMD5": getMD5(bypassStops),
        "updatedAt": datetime.now(pytz.timezone('Asia/Seoul')), #2023-10-20 13:00:32.447078+09:00
    }

# 모든 노선의 우회 정류장 리스트 반환
def getAllBypassStops():
    allBypassStops = []
    for bus in BUS_LIST:
        allBypassStops.append(getBusBypassStops(bus["busId"], bus["busName"]))
    return allBypassStops

def getBusRoute(): 
    try:
        client = MongoClient(os.getenv('MONGODB_ADDRESS'))
        noticeDB = client["smus"]
        routeTable = noticeDB["bus_route"]
        
        allBusBypassStops = getAllBypassStops()
        print(len(allBusBypassStops))

        routeTable.drop()
        for i in allBusBypassStops:
            routeTable.insert_one(i)
            
    except Exception as e:
        print(e)
        sendMessageToSlack(getNormalErrorMessage(e))
        print("Crawling Failed. Time:", str(datetime.now(pytz.timezone('Asia/Seoul'))))
        
    print("Crawling Done. Time:", str(datetime.now(pytz.timezone('Asia/Seoul'))))

if __name__ == "__main__":
    getBusRoute()
    schedule.every(30).minutes.do(getBusRoute)
    while True:
            schedule.run_pending()
            time.sleep(1)
