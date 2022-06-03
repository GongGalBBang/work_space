import sys
import logging
import pymysql
import dbinfo
import json
import datetime
import pprint

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
    user_name = event["name"]
    user_id = event["user_id"]
    user_pw = event["user_pw"]
    department = event["department"]
    circle = event["circle"]
    
    cursor = connection.cursor()
    cursor.execute("select * from Gonggalbbang.Profile")
    rows = cursor.fetchall()
    
    check = 0
    for row in rows:
        if row[2] == user_id:
            check = 1
    
    if check == 0:
        cursor.execute("INSERT INTO Profile (Name, Email, PW, Department, circle) VALUES (%s,%s,%s,%s,%s);",(user_name, user_id, user_pw, department, circle))
        connection.commit()
        
        return {
            'statusCode': 200,
            'body': json.dumps('success making ID')
        }
    else:
        return {
            'statusCode': 409,
            'body': json.dumps('already exist ID')
        }
