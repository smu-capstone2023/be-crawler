# Python 3.8.10 - 64-bit

# pip3 install beautifulsoup4
# pip3 install requests
# pip3 install pymongo
# pip3 install schedule
# pip3 install google-cloud-vision
# pip3 install python-dotenv
# pip3 install pytz
# pip3 install PyMuPDF

# DB - database name: smus
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

import fitz
import base64
import hashlib
from google.cloud import vision
import os
from dotenv import load_dotenv

import urllib3 # ssl 연결 경고 무시하기
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

load_dotenv()

BUS_NUMBER_LIST = ["7016", "1711", "163", "서대문08", "종로13", "7016번", "1711번", "163번", "서대문08번", "종로13번"]
BUS_NUMBER_SET = set(BUS_NUMBER_LIST)

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
        "Level": "error",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "WarningMessage": "예상치 못한 에라가 발생해 이번 크롤링이 실패했습니다. ",
        "data": issueData
    }
    
    return json.dumps(errorMessage, ensure_ascii = False)

# 이미지 다운로드 재시도 경고 메시지(준비중)
def getFileDownloadRetryMessage(fileUrl, retryTimes, error):
    issueData = {
        "imageURL": fileUrl,
        "retryTimes": retryTimes,
        "error": str(error)
    }
    warnMessage = {
        "Level": "warning",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "WarningMessage": "파일을 다운로드할 때 연결을 거부당해서 재시도 중입니다. 크롤러가 너무 빈번하게 작동했거나 기존 연결을 끊지 않을 가능성이 있습니다. ",
        "data": issueData
    }
    
    return json.dumps(warnMessage, ensure_ascii = False)

# 이미지 다운로드 실패 에러 메시지
def getFileDownloadErrorMessage(NoticeId, error):
    issueData = {
        "imageURL": NoticeId,
        "error": str(error)
    }
    errorMessage = {
        "Level": "error",
        "ErrorHost":"BUS Crawling GCP",
        "Time": str(datetime.now(pytz.timezone('Asia/Seoul'))), #2023-10-20 13:00:32.447078+09:00
        "WarningMessage": "사진을 다운로드할 때 연결을 거부당해서 살패했습니다. 크롤러가 너무 빈번하게 작동했거나 기존 연결을 끊지 않을 가능성이 있습니다. ",
        "data": issueData
    }
    return json.dumps(errorMessage, ensure_ascii = False, indent=2)

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

# '우회' 라는 단어가 들어간 공지사항의 id들을 받아오기
def getNotices():
    baseURL = f"https://topis.seoul.go.kr/notice/selectNoticeList.do"
    baseBody = {
        "pageIndex": 1,
        "recordPerPage": 10,
        "category": "sTtl",
        "boardSearch": "우회",
        "crawlingTime": str(datetime.now(pytz.timezone('Asia/Seoul')))
    }

    responseJsonString = requests.post(baseURL, data=baseBody, verify=False).content
    noticeDict = json.loads(responseJsonString)

    notices = []
    for info in noticeDict["rows"]:
        noticeId = info["bdwrSeq"]
        createdTime = convertTime(info["createDate"])
        updatedTime = convertTime(info['updateDate'])
        title = info['bdwrTtlNm']
        ContentSoup = bs(info["bdwrCts"], "html.parser")
        ContentText = ""
        for nowTag in ContentSoup.children:
            if nowTag.text != "":
                    ContentText+=nowTag.text + "\n"
        
        notices.append({
            "number": noticeId,
            "createdTime": createdTime,
            "updatedTime": updatedTime,
            "title": title,
            "content": ContentText,
        })
    return notices

# 첨부파일 다운로드하기
def downloadFiles(noticeId):
    noticeId = str(noticeId)
    # 현재 noticeId으로 폴더 만들기
    os.mkdir(f'./{noticeId}')
    print("Download Notice File: ", noticeId)
    
    baseURL = f"https://topis.seoul.go.kr/notice/selectNoticeFileDown.do"
    baseBody = {
        "bdwrSeq": noticeId
    }
    
    filePaths = []
    try:
        filesResponse = requests.post(baseURL, data=baseBody, verify=False).content
        fileDict = json.loads(filesResponse)
        for fileDate in fileDict["rows"]:
            if fileDate["apndFile"] == None: continue
            fileBytes = bytes(fileDate["apndFile"], 'utf-8')
            fileExt = fileDate['apndFileNm'].rsplit('.', 1)[1]
            fileName = hashlib.md5(fileBytes).hexdigest() + "." + fileExt
            filePath = f"./{noticeId}/{fileName}"
            with open(filePath, 'wb') as handler:
                handler.write(base64.decodebytes(fileBytes))
            filePaths.append(filePath)
    except Exception as e:
        message = getFileDownloadErrorMessage(noticeId, e)
        sendMessageToSlack(message)
        raise Exception(message)
    return filePaths

# pdf 파일을 각각의 사진으로 자르기
def pdf2images(pdfPath):
    pdfPath = os.path.abspath(pdfPath)
    pdfName = os.path.basename(pdfPath).rsplit('.', 1)[0]
    
    folderDir = os.path.dirname(pdfPath)
    imageFolderPath = os.path.join(folderDir, 'images')
    if not os.path.exists(imageFolderPath):
        os.mkdir(imageFolderPath)
    pdfImagesFolderPath = os.path.join(imageFolderPath, pdfName)
    os.mkdir(pdfImagesFolderPath)
    
    doc = fitz.open(pdfPath)
    for i, page in enumerate(doc):
        newImagePath = os.path.join(pdfImagesFolderPath, f"{i}.png")
        img = page.get_pixmap()
        img.save(newImagePath)
    return pdfImagesFolderPath

# ocr 요청하고 set으로 반환해주기
def detect_text(imagePath):
    """Detects text in the file."""
    client = vision.ImageAnnotatorClient()

    with open(imagePath, "rb") as image_file:
        content = image_file.read()

    image = vision.Image(content=content)

    response = client.document_text_detection(image=image)
    if response.error.message:
        message = getOCRErrorMessage(imagePath, response.error.message)
        raise Exception(message)

    texts = response.text_annotations[0].description
    textsList = texts.split("\n")
    textSet = set(textsList)
    return textSet

# pdf 파일을 ocr 인식
def googleOcrPdf(pdfPath):
    imagesFolder = pdf2images(pdfPath)
    resultSet = set()
    for root, dirs, files in os.walk(imagesFolder):
        for imageName in files:
            imagePath = os.path.join(root, imageName)
            ocrResult = detect_text(imagePath)
            resultSet = resultSet.union(ocrResult)
            os.remove(imagePath)
    os.rmdir(imagesFolder)
    return resultSet

# db에 새로운 url 및 대응하는 set형으로 저장. TTL 추가 예정
def saveSetResult(dbTable, filePath, resultSet): 
    fileName = os.path.basename(filePath).rsplit('.', 1)[0]
    dbTable.insert_one({
        "file_name": fileName,
        "bus_number": list(resultSet)
    })

# 해당 url이 db에 존재하면 set형, 없으면 None반환
def getResultFromHistoryDB(historyTable, filePath):
    fileName = os.path.basename(filePath).rsplit('.', 1)[0]
    history = historyTable.find_one({"file_name": fileName})
    if history != None:
        print("Found History: ", fileName)
        return set(history['bus_number'])
    else: 
        return None

# 파일 경로로 OCR를 요청하기
def getResultFromFile(filePath):
    googleOcrSet = googleOcrPdf(filePath)
    return googleOcrSet & BUS_NUMBER_SET

# 파일 경로를 입력하고 set형을 반환하기
def getNumberSet(historyTable, filePath):
    busResultSet = getResultFromHistoryDB(historyTable, filePath)
    if busResultSet == None:
        busResultSet = getResultFromFile(filePath)
        saveSetResult(historyTable, filePath, busResultSet)
    return busResultSet

def getBusNotice(): 
    try:
        client = MongoClient(os.getenv('MONGODB_ADDRESS'))
        noticeDB = client["smus"]
        noticeTable = noticeDB["bus_notice"]
        ocrHistoryTable = noticeDB["ocr_history"]
        
        results = []
        busNotices = getNotices()
        for noticeInfo in busNotices:
            noticeId = noticeInfo['number']
            noticeFilePaths = downloadFiles(noticeId)
            noticeResultSet = set()
            for filePath in noticeFilePaths:
                busNumberSet = getNumberSet(ocrHistoryTable, filePath)
                print("busNumberSet: ", busNumberSet)
                noticeResultSet = noticeResultSet.union(busNumberSet)
                os.remove(filePath)
                
            if os.path.exists(f'./{noticeId}'):
                if os.path.exists(f'./{noticeId}/images'):
                    os.rmdir(f'./{noticeId}/images')
                os.rmdir(f'./{noticeId}')
                
            noticeInfo['bus_number'] = list(noticeResultSet)
            results.append(noticeInfo)
            print("==========")

        noticeTable.drop()
        for i in results:
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
