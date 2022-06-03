import sys
import logging
import pymysql
import dbinfo
import json
import datetime

#오류시 좀 더 정확한 정보 얻기 위해 logging 함수 사용
log = logging.getLogger()
log.setLevel(logging.INFO)

#저장된 데이터로 DB 연결
connection = pymysql.connect(
    host = dbinfo.db_host,
    user = dbinfo.db_username,
    password = dbinfo.db_password,
    db = dbinfo.db_name,
    port = dbinfo.db_port)
    
def lambda_handler(event, context):
    
    user_id = event["user_id"]
    user_pw = event["user_pw"]
    
    cursor = connection.cursor()    #연결된 db의 cursor 객체 생성
    cursor.execute("select * from Gonggalbbang.Profile")   #dataTable의 위치에는 schema 이름이 들어가야 함. time 기준 오름차순 정렬 
    rows = cursor.fetchall()    #cursor 객체를 통해 데이터를 DB에서 불러옴. fetch된 데이터 사용.
    
    
    for row in rows:
        if user_id == row[2]:
            if user_pw == row[3]:
                #한국어 반환값의 ubc15등은 unicode의 bc15 
                user_data = dict(name = row[1], club = row[5], major = row[4])
                
                return  {
                    'statusCode': 200,
                    'body': json.dumps(user_data)
                }
            else:
                return  {
                    'statusCode': 403,
                    'body': json.dumps('Wrong Password')
                }
    
    return  {
        'statusCode': 403,
        'body': json.dumps('Wrong ID')
    }
