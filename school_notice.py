# Python 3.8.10 - 64-bit

# pip3 install beautifulsoup4
# pip3 install requests
# pip3 install pymongo
# pip3 install schedule
# pip3 install pytz

# DB - database name: smus
# DB - collection name: school_notice

import requests
from bs4 import BeautifulSoup as bs
from pymongo import MongoClient

import schedule
import time
from datetime import datetime

import os
import json
import pytz
from dotenv import load_dotenv
load_dotenv()

# 슬랙으로 메시지 보내기
def sendMessageToSlack(message):
    SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
    SLACK_ERROR_CHANNEL = os.getenv('SLACK_ERROR_CHANNEL')

    requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer " + SLACK_BOT_TOKEN},
        data={"channel": SLACK_ERROR_CHANNEL,"text": message})

def getSchoolNotice(): 
    try: 
        client = MongoClient(os.getenv('MONGODB_ADDRESS'))
        noticeDB = client["smus"]
        noticeTable = noticeDB["school_notice"]

        campus = "smu" #천안캠은 smuc
        page = requests.get(f'https://www.smu.ac.kr/kor/life/notice.do?srUpperNoticeYn=on&srCampus={campus}&article.offset=0&articleLimit=100')

        soup = bs(page.content, "html.parser")

        table_tag = soup.find('ul', class_="board-thumb-wrap")
        noticeList = table_tag.find_all("dl")

        noticeResultList = []
        for i in noticeList:
            NOW_TITLE = i.dt.table.tbody.find_all("td")[2].text.replace("\t", "").replace("\r", "").replace("\n", "")
            NOW_INDEX = i.dd.ul.find_all('li')[0].text.replace("\t", "").replace("\r", "").replace("\n", "").replace("No.", "")
            # NOW_AUTHOR = i.dd.ul.find_all('li')[1].text.replace("\t", "").replace("\r", "").replace("\n", "").replace("작성자", "")
            NOW_DATE = i.dd.ul.find_all('li')[2].text.replace("\t", "").replace("\r", "").replace("\n", "").replace("작성일", "")
            SITE_DATE_LIST = NOW_DATE.split('-')
            SERVER_TIME = f'{SITE_DATE_LIST[0]}.{SITE_DATE_LIST[1]}.{SITE_DATE_LIST[2]}_00:00:00' # 2023.04.02_01:06:23
            NOW_VIEWS = i.dd.ul.find_all('li')[3].text.replace("\t", "").replace("\r", "").replace("\n", "").replace("조회수", "")
            noticeResultList.append({
                "title": NOW_TITLE,
                "postId": int(NOW_INDEX),
                "createdTime": SERVER_TIME,
                "views": int(NOW_VIEWS)
            })
        
        noticeTable.drop()
        for i in noticeResultList:
            noticeTable.insert_one(i)
    except Exception as e:
        print(e)
        sendMessageToSlack(e)
        print("Crawling Failed. Time:", str(datetime.now(pytz.timezone('Asia/Seoul'))))
        
    print("Crawling Done. Time:", str(datetime.now(pytz.timezone('Asia/Seoul'))))
    
if __name__ == "__main__":
    getSchoolNotice()
    schedule.every().hour.do(getSchoolNotice)
    while True:
            schedule.run_pending()
            time.sleep(1)
