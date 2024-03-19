# Python 3.8.10 - 64-bit

# pip3 install beautifulsoup4
# pip3 install requests
# pip3 install pymongo
# pip3 install schedule
# pip3 install python-dotenv
# pip3 install pytz

# DB - database name: smus
# DB - collection name: bus_route

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
    {"busName": "7016", "busId": "100100447", "notificationType": '7016'},
    {"busName": "1711", "busId": "100100185", "notificationType": '1711'},
    {"busName": "163", "busId": "100100032", "notificationType": '163'},
    {"busName": "서대문08", "busId": "100900012", "notificationType": 'Seodaemun08'},
    {"busName": "종로13", "busId": "100900002", "notificationType": 'Jongro13'}
]

HISTORY_FILE_NAME = 'history.json'
def getHistoryDir():
    return os.getcwd() + "/" + HISTORY_FILE_NAME

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

# 알림 API 후출하기
def sendDetourStartNotificationToSMUS(busName, notificationType):
    baseURL = f"https://develop.smus.co.kr/api/notification/detourStart"
    body = {
        'key': os.getenv('SMUS_NOTIFICATION_KEY'),
        'busName': busName,
        'notificationType': notificationType
    }
    return requests.post(baseURL, data=body, verify=False).content

def sendDetourUpdateNotificationToSMUS(busName, notificationType):
    baseURL = f"https://develop.smus.co.kr/api/notification/detourUpdate"
    body = {
        'key': os.getenv('SMUS_NOTIFICATION_KEY'),
        'busName': busName,
        'notificationType': notificationType
    }
    return requests.post(baseURL, data=body, verify=False).content

def sendDetourFinishNotificationToSMUS(busName, notificationType):
    baseURL = f"https://develop.smus.co.kr/api/notification/detourFinish"
    body = {
        'key': os.getenv('SMUS_NOTIFICATION_KEY'),
        'busName': busName,
        'notificationType': notificationType
    }
    return requests.post(baseURL, data=body, verify=False).content

# history파일 읽기 함수 -> "버스노선이름":"최근의 md5정보"인 dict 반환
def readHistoryFileToDict():
    if not os.path.exists(getHistoryDir()):
        f = open(getHistoryDir(), 'w')
        f.close()
        return {}
    busMd5Dict = {}
    f = open(getHistoryDir(), 'r')
    busMd5StringList = f.readlines()
    for busMd5String in busMd5StringList:
        busData = busMd5String[:-1].split(":")
        busMd5Dict[busData[0]] = busData[1]
    return busMd5Dict

# history파일 쓰기 함수 -> dict 정보를 "버스노선이름":"최근의 md5정보"로 쓰기
def writeDictToHistoryFile(md5Dict):
    fileString = ''
    for k, v in md5Dict.items():
        fileString+= k + ":" + v + "\n"

    f = open(getHistoryDir(), 'w')
    f.write(fileString)
    f.close()

# 현재 노선 및 md5 정보를 넘길 때 기존과 비교하고, 어떤 알림을 보내야 할 지 결정하기
def sendNotificationByNewInfomation(busName, notificationType, busStops, oldMd5Dict):
    oldMd5 = None
    if busName in oldMd5Dict:
        oldMd5 = oldMd5Dict[busName]
    
    if oldMd5 == None:
        if len(busStops) > 0: 
            # 우회역 새로 발생한다는 알림 보내기
            sendDetourStartNotificationToSMUS(busName, notificationType)
            return
    else:
        if len(busStops) == 0: 
            # 우회역 사라짐 -> 우회 종료 알림 보내기
            sendDetourFinishNotificationToSMUS(busName, notificationType)
            return
        else:
            newMd5 = getMD5(busStops)
            if oldMd5 != newMd5:
                # 우회역이 기존에 비해 더 추가됐다 -> 우회역 업데이트 알림 보내기
                sendDetourUpdateNotificationToSMUS(busName, notificationType)
                return
    return

# 우회 정류장 데이터의 MD5값 계산 -> 데이터 변화 여부 확인 위함.
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

# 모든 노선의 우회 정류장 리스트 반환
def getAllBypassStops():
    oldMd5Dict = readHistoryFileToDict()
    newMd5Dict = {}
    allBypassStops = []
    for bus in BUS_LIST:
        busId = bus["busId"]
        busName = bus["busName"]
        notificationType = bus['notificationType']
        xmlString = requestBusStopsApiByRoute(busId)
        bypassStops = parseBusStopXml(xmlString)
        sendNotificationByNewInfomation(busName, notificationType, bypassStops, oldMd5Dict)
        
        newMd5 = None
        if len(bypassStops) > 0:
            newMd5 = getMD5(bypassStops)
            newMd5Dict[busName] = newMd5
            
        allBypassStops.append({
            "busName": busName,
            "busId": busId,
            "stops": bypassStops,
            "stopsMD5": newMd5,
            "updatedAt": datetime.now(pytz.timezone('Asia/Seoul')), #2023-10-20 13:00:32.447078+09:00
        })
        
    writeDictToHistoryFile(newMd5Dict)
    return allBypassStops

# 크롤링 함수
def getBusRoute(): 
    try:
        client = MongoClient(os.getenv('MONGODB_ADDRESS'))
        noticeDB = client["smus"]
        routeTable = noticeDB["bus_route"]

        allBusBypassStops = getAllBypassStops()
        
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
