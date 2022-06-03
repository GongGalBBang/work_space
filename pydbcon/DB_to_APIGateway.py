import sys
import logging
import pymysql
import dbinfo
import json
import datetime
import pprint
#주의사항! 더미데이터 셋과 줄울림 셋의 타임 맞지 않아 더미데이터 셋으로 연결해 두었음!


#동아리, 전공 데이터
club = ("null", "Jullulim", "PlantsClub", "ProgrammingClub", "BoardGameClub", "AncientHistoryClub")
major = ("AerospaceEngineering", "AiSystem", "MechanicalEngineering", "SmartDrone", "SoftWare")
day_check = (
                datetime.datetime(2022,5,16, 10, 0, 0), 
                datetime.datetime(2022,5,17, 10, 0, 0), 
                datetime.datetime(2022,5,18, 10, 0, 0), 
                datetime.datetime(2022,5,19, 10, 0, 0), 
                datetime.datetime(2022,5,20, 10, 0, 0), 
                datetime.datetime(2022,5,21, 10, 0, 0), 
                datetime.datetime(2022,5,22, 10, 0, 0)
            )


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

def database_access(index, array, date_info):
    access_time = date_info - datetime.timedelta(hours=1)
    next_time = access_time + datetime.timedelta(days=1)
    
    access_timeStr = json.dumps(access_time, default=json_default)
    next_timeStr = json.dumps(next_time, default=json_default)
        
    condition_sentence = "select * from Gonggalbbang." + array[index] + " WHERE Time >= " + access_timeStr + " AND Time < " + next_timeStr + " order by time asc"
    return condition_sentence

#파이썬 데이터 형식 datetime을 json이 읽지 못하므로 타입을 스트링으로 변경하는 함수
def json_default(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%Y-%m-%d %H:%M:%S')
    raise TypeError('not JSON serializable')
 
def json_time(value):
    if isinstance(value, datetime.datetime):
        return value.strftime('%H:%M:%S')
    raise TypeError('not JSON serializable')
    
def data_calculator(access_time, num, case):
    
    date_info = day_check[access_time.weekday()].replace(hour = 10, minute =0, second = 0, microsecond = 0)
    date_end = date_info.replace(hour = 22)
    per_sum = 0
    per_num = 0
    cursor = connection.cursor()    #연결된 db의 cursor 객체 생성
    
    if case == 0 and num == 0:
        dummy = datetime.datetime(2022, 5, 21, 00, 58, 23)
        access_rows = [[0, -1, dummy]]
    elif case == 0:
        cursor.execute(database_access(num, club, date_info))   #불러올 club의 조건 명령문 불러오기
        access_rows = cursor.fetchall()    #cursor 객체를 통해 데이터를 DB에서 불러옴. fetch된 데이터 사용.
    elif case == 1:
        cursor.execute(database_access(num, major, date_info))   #불러올 club의 조건 명령문 불러오기
        access_rows = cursor.fetchall()    #cursor 객체를 통해 데이터를 DB에서 불러옴. fetch된 데이터 사용.
    inst_rows = []
    
    for row in access_rows:
        if row[1] == -1:
            continue
        elif date_info > row[2]:       # 1시간 동안의 데이터를 참조
            per_sum = per_sum + row[1]
            per_num = per_num + 1
        elif date_info < date_end:               # 1시간 이후의 데이터를 받았으면 이전 데이터는 연산 후 저장, 다음 데이터 준비
            if per_num != 0:
                inst_row = [date_info, per_sum/per_num]
                inst_rows.append(inst_row)
                date_info = date_info + datetime.timedelta(hours=1)
            else:
                while date_info <= row[2]: 
                    inst_row = [date_info, -1]
                    inst_rows.append(inst_row)
                    date_info = date_info + datetime.timedelta(hours=1)
            per_sum = row[1]
            per_num = 1
            
    if per_num != 0 and date_info < date_end:
        inst_row = [date_info, per_sum/per_num]
        inst_rows.append(inst_row)
        date_info = date_info + datetime.timedelta(hours=1)
    
    if date_info < date_end:
        while date_info < date_end:
            inst_row = [date_info, -1]
            inst_rows.append(inst_row)
            date_info = date_info + datetime.timedelta(hours=1)

    json_lists = []
    
    for row in inst_rows:    #불러온 데이터 전부 출력
        json_list = dict(date = json.dumps(row[0], default = json_time).strip('"'), member = row[1])
        json_lists.append(json_list)
    
    json_object = json.dumps(json_lists)

    json_return = dict(className = (case+1)*100 + num, data = json_object)

    return json_return

def lambda_handler(event, context):
    access_time = datetime.datetime.strptime(event['access_time'], "%Y-%m-%d %H:%M:%S")
    club_num = list(event['club_number'].strip("[]").replace(" ", "").split(','))
    major_num = list(event['major_number'].strip("[]").replace(" ", "").split(','))
    print(club_num)
    
    
    return_rows = []
    for i in club_num:
        return_rows.append(data_calculator(access_time, int(i), 0))
    for i in major_num:
        return_rows.append(data_calculator(access_time, int(i), 1))
    
    
    
    pprint.pprint(return_rows, width=80)
    
    return {
        'statusCode': 200,
        'body': json.dumps(return_rows)
        }
