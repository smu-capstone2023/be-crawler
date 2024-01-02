# Python 3.8.10 - 64-bit

# pip3 install beautifulsoup4
# pip3 install requests
# pip3 install pymongo
# pip3 install schedule
# pip3 install google-cloud-vision
# pip3 install python-dotenv
# pip3 install pytz

# DB - database name: school_notice
# DB - collection name: bus_notice
# DB - OCR collection name: ocr_history

import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

import schedule
import time
import json

from datetime import datetime
import pytz

from google.cloud import vision
import os
from dotenv import load_dotenv

import urllib3 # ssl 연결 경고 무시하기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

BUS_NUMBER_LIST = ["7016", "1711", "163", "서대문08", "종로13"]
BUS_NUMBER_SET = set(BUS_NUMBER_LIST)

def convertTime(timeString):
    return datetime.strptime(timeString, "%Y-%m-%d %H:%M:%S")

# set형을 json 배열의 문자열 형태로 반환
def setObject2JsonArrayString(setObject):
    newStringList = []
    for i in setObject:
        newStringList.append('"' + i + '"')
    return "[" + ", ".join(newStringList) + "]"

# json 배열의 문자열을 set형태로 반환
def jsonArrayString2SetObject(jsonString):
    listObjest = json.loads(jsonString)
    return set(listObjest)

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
        "Level": "error",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "WarningMessage": "예상치 못한 에라가 발생해 이번 크롤링이 실패했습니다. ",
        "data": issueData
    }
    
    return json.dumps(errorMessage, ensure_ascii = False)

# 이미지 다운로드 재시도 경고 메시지(준비중)
def getImageDownloadRetryMessage(imgURL, retryTimes, error):
    issueData = {
        "imageURL": imgURL,
        "retryTimes": retryTimes,
        "error": str(error)
    }
    warnMessage = {
        "Level": "warning",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "WarningMessage": "사진을 다운로드할 때 연결을 거부당해서 재시도 중입니다. 크롤러가 너무 빈번하게 작동했거나 기존 연결을 끊지 않을 가능성이 있습니다. ",
        "data": issueData
    }
    
    return json.dumps(warnMessage, ensure_ascii = False)

# 이미지 다운로드 실패 에러 메시지
def getImageDownloadErrorMessage(imgURL, error):
    issueData = {
        "imageURL": imgURL,
        "error": str(error)
    }
    errorMessage = {
        "Level": "error",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "WarningMessage": "사진을 다운로드할 때 연결을 거부당해서 살패했습니다. 크롤러가 너무 빈번하게 작동했거나 기존 연결을 끊지 않을 가능성이 있습니다. ",
        "data": issueData
    }
    return json.dumps(errorMessage, ensure_ascii = False)

# 이미지 OCR 실패 에러 메시지
def getOCRErrorMessage(imagePath, error):
    issueData = {
        "imagePath": imagePath,
        "error": str(error),
    }
    errorMessage = {
        "Level": "error",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "ErrorMessage": "OCR 과정에서 에러가 생겨 인식이 중단되었습니다. GCP 결제 오류 또는 인터넷 문제일 가능성이 있습니다. ",
        "data": issueData
    }
    return json.dumps(errorMessage, ensure_ascii = False)

# ocr 사진 다운로드하기
def downloadImage(url):
    print("Start OCR: ", url)
    imageName = url.split("/")[-1]
    imagePath = f"./{imageName}"
    
    try:
        img_data = requests.get(url, verify=False).content
    except Exception as e:
        message = getImageDownloadErrorMessage(url, e)
        sendMessageToSlack(message)
        raise Exception(message)
    
    with open(imagePath, 'wb') as handler:
        handler.write(img_data)
        return imagePath

# ocr 사진 삭제하기  
def deleteImage(imagePath):
    os.remove(imagePath)

# ocr 요청하고 set으로 반환해주기
def detect_text(path):
    """Detects text in the file."""
    client = vision.ImageAnnotatorClient()

    with open(path, "rb") as image_file:
        content = image_file.read()

    image = vision.Image(content=content)

    response = client.document_text_detection(image=image)
    if response.error.message:
        message = getOCRErrorMessage(path, response.error.message)
        raise Exception(message)

    texts = response.text_annotations[0].description
    textsList = texts.split("\n")
    textSet = set(textsList)
    return textSet

# 실제 ocr 전체 과정
def googleOcrUrl(url):
    imagePath = downloadImage(url)
    resultSet = detect_text(imagePath)
    deleteImage(imagePath)
    return resultSet

# db에 새로운 url 및 대응하는 set형으로 저장. TTL 추가 예정
def saveSetResult(dbTable, url, resultSet): 
    dbTable.insert_one({
        "url": url,
        "bus_number": setObject2JsonArrayString(resultSet)
    })
    
# 해당 url이 db에 존재하면 set형, 없으면 None반환
def getResultFromDB(dbTable, url):
    dbResult = dbTable.find_one({"url": url})
    if dbResult != None:
        print("find History: ", url)
        return jsonArrayString2SetObject(dbResult['bus_number'])
    else: 
        return None

# URL으로 OCR를 요청하고, 그 결과를 Set형 버스 번호 정보로 반환 및 DB 저장하기
def getResultFromUrl(urlTable, url):
    googleOcrResult = googleOcrUrl(url)
    matchedBusNumberSet = set(googleOcrResult) & BUS_NUMBER_SET
    saveSetResult(urlTable, url, matchedBusNumberSet)
    return matchedBusNumberSet

# url를 입력하고 set형을 반환하기
def getNumberSet(urlTable, url):
    busResultSet = getResultFromDB(urlTable, url)
    if busResultSet == None:
        busResultSet = getResultFromUrl(urlTable, url)
    return busResultSet

def getBusNotice(): 
    try:
        client = MongoClient("mongodb://admin:capstone@localhost:27017/")
        noticeDB = client["school_notice"]
        noticeTable = noticeDB["bus_notice"]
        ocrHistoryTable = noticeDB["ocr_history"]
        
        baseURL = f"https://topis.seoul.go.kr/notice/selectNoticeList.do"
        baseBody = {
            "pageIndex": 1,
            "recordPerPage": 10,
            "category": "sTtl",
            "boardSearch": "우회"
        }
        
        responseJsonString = requests.post(baseURL, data=baseBody, verify=False).content
        noticeDict = json.loads(responseJsonString)
        
        newBusInfoList = []
        for info in noticeDict["rows"]:
            number = info["bdwrSeq"]
            createdTime = convertTime(info["createDate"])
            updatedTime = convertTime(info['updateDate'])
            title = info['bdwrTtlNm']
            
            ContentSoup = bs(info["bdwrCts"], "html.parser")
            ContentText = ""
            
            # 이미지 링크 크롤링
            imgTags = ContentSoup.find_all("img")
            imgUrlList = []
            for img in imgTags:
                nowImageUrl = 'https://' + img["src"][2:]
                if img["src"].startswith('https://'):
                    nowImageUrl = img['src']
                elif img["src"].startswith('http://'):
                    nowImageUrl = 'https://' + img["src"][7:]
                imgUrlList.append(nowImageUrl)
            imgUrlListString = setObject2JsonArrayString(imgUrlList)
            
            # 이미지 링크 OCR 돌리기
            BUS_NUMBER_OCR_RESULT = set()
            for imageUrl in imgUrlList:
                resultSet = getNumberSet(ocrHistoryTable, imageUrl)
                BUS_NUMBER_OCR_RESULT = BUS_NUMBER_OCR_RESULT | resultSet
            busNumberOcrResultString = setObject2JsonArrayString(BUS_NUMBER_OCR_RESULT)
            print("OCR결과: ", busNumberOcrResultString)
                
            # 게시글 상세 내용 크롤링
            for nowTag in ContentSoup.children:
                if nowTag.text != "":
                        ContentText+=nowTag.text + "\n"
                   
            newBusInfoList.append({
                "number": number,
                "createdTime": createdTime,
                "updatedTime": updatedTime,
                "title": title,
                "imageUrlList": imgUrlListString,
                "content": ContentText,
                "bus_number_list": busNumberOcrResultString
            }) 
            print("==========")
        

        noticeTable.drop()
        for i in newBusInfoList:
            noticeTable.insert_one(i)
            
    except Exception as e:
        print(e)
        sendMessageToSlack(e)
        print("Crawling Failed. Time:", str(datetime.now(pytz.timezone('Asia/Seoul'))))
        
    print("Crawling Done. Time:", str(datetime.now(pytz.timezone('Asia/Seoul'))))

if __name__ == "__main__":
    getBusNotice()
    schedule.every().hour.do(getBusNotice)
    while True:
            schedule.run_pending()
            time.sleep(1)
