import sys
import logging
import pymysql
import dbinfo
import json

#저장된 데이터로 DB 연결
connection = pymysql.connect(
    host = dbinfo.db_host,
    user = dbinfo.db_username,
    password = dbinfo.db_password,
    db = dbinfo.db_name,
    port = dbinfo.db_port)

def lambda_handler(event, context):
    cursor = connection.cursor()    #연결된 db의 cursor 객체 생성
    cursor.execute("select * from dataTable")   #dataTable의 위치에는 DB table 명이 들어가야 함
    rows = cursor.fetchall()    #cursor 객체에서 데이터를 서버에서 불러옴. fetch된 데이터 사용.

    print(rows) #불러온 데이터 전부 출력
