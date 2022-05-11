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

#파이썬 데이터 형식 datetime을 json이 읽지 못하므로 타입을 스트링으로 변경하는 함수
def json_default(value):
    if isinstance(value, datetime.time):
        return value.strftime('%H:%M:%S')
    raise TypeError('not JSON serializable')

def lambda_handler(event, context):
    date_now = datetime.datetime(2022, 5, 11, 0, 0, 0, 0)
    
    cursor = connection.cursor()    #연결된 db의 cursor 객체 생성
    cursor.execute("select * from dummy_data.dummy_table")   #dataTable의 위치에는 schema 이름이 들어가야 함
    rows = cursor.fetchall()    #cursor 객체를 통해 데이터를 DB에서 불러옴. fetch된 데이터 사용.
    
    #convert_rows에 rows의 datatime 타입 변수를 스트링으로 교체 후 저장
    convert_rows = []
    
    
    '''최초 데이터가 필요 데이터가 되도록 데이터 내용을 자르는 부분을 추가해야 함'''
    '''에러가 걱정되는 부분 - 만약 1시간 전부가 에러데이터라면?'''
    
    # 데이터 연산 후 저장
    #최초 데이터 확인 후 최초부터 1시간 이내 데이터까지 계산
    date_info = rows[0][0]
    date_info + datetime.timedelta(hours=1)
    for row in rows:
        # 받은 데이터의 개수와 총합을 계산
        per_sum = 0
        per_num = 0
        if: row[1] == -1:   #에러 데이터는 넘기기
            continue
        elif row[0] < date_info:        # 1시간 동안의 데이터를 참조
            per_aver = per_sum + row[1]
            per_num = per_num + 1
        else:               # 1시간 이후의 데이터를 받았으면 이전 데이터는 연산 후 저장, 다음 데이터 준비
            convert_row = [json.dumps(date_info.time(), default=json_default), per_sum/per_num]
            convert_rows.append(convert_row)
            date_info + datetime.timedelta(hours=1)
            per_aver = row[1]
            per_num = 1
    # 아직 연산되지 못한 마지막 데이터 저장
    if per_num != 0:
        convert_row = [json.dumps(date_info.time(), default=json_default), per_sum/per_num]
        convert_rows.append(convert_row)
            
    
    print(convert_rows)    #불러온 데이터 전부 출력
    
    return convert_rows     #불러온 데이터 전부 반환
    
