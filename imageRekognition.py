#Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#PDX-License-Identifier: MIT-0 (For details, see https://github.com/awsdocs/amazon-rekognition-developer-guide/blob/master/LICENSE-SAMPLECODE.)
import boto3
import datetime
import urllib.parse
import pymysql
import json

def connect_RDS():
    with open("dbinfo.json", "r") as f:
        dbinfo = json.load(f)

    conn = pymysql.connect(
        host=dbinfo['host'],
        user=dbinfo['username'],
        passwd=dbinfo['password'],
        db=dbinfo['database'],
        port=dbinfo['port']
    )
    return conn

def send_RDS(conn, person_count, imageDateTime,coordinate):
    cur = conn.cursor()
    cur.execute("INSERT INTO Jullulim (person_count, Time, coordinate) VALUES (%s,%s,%s);",(person_count, imageDateTime, coordinate))
    conn.commit()


def detect_labels(photo, bucket):
    client=boto3.client('rekognition')
    
    #최소 신뢰도 95%로 지정하여 detect_labels 호출. MaxLabels은 응답에 반환 될 최대 label 수.
    response = client.detect_labels(Image={'S3Object':{'Bucket':bucket,'Name':photo}}, MinConfidence=95)
    
    #인원수 세어요
    countPerson = 0
    #좌표값을 저장해요
    coordinate = [] 
    
    print('Detected labels for ' + photo) 
    print()  
    
    #검출된 라벨(사람, 자동차 등)
    for label in response['Labels']:
        print ("Label: " + label['Name'])
        print ("Confidence: " + str(label['Confidence']))
        print ("Instances:")
        
        #라벨의 인스턴스들 (각 개체별 box 좌표값을 프린트 함)
        for instance in label['Instances']:
            
            #person이면
            if(label['Name'] == 'Person'):
                #인원수 +1
                countPerson = countPerson + 1
                print ("countPerson : ", countPerson)
                
                #비율로 표기된 좌표, 높이, 너비 값에 100을 곱하고 소수점 아래 버림
                top = int(instance['BoundingBox']['Top'] * 100)
                left = int(instance['BoundingBox']['Left'] * 100)
                width = int(instance['BoundingBox']['Width'] * 100)
                height = int(instance['BoundingBox']['Height'] * 100)
                #좌표 리스트에 추가
                coordinate.append([top, left, width, height])
                
            print ("  Bounding box")
            print ("    Top: " + str(instance['BoundingBox']['Top']))
            print ("    Left: " + str(instance['BoundingBox']['Left']))
            print ("    Width: " +  str(instance['BoundingBox']['Width']))
            print ("    Height: " +  str(instance['BoundingBox']['Height']))
            print ("  Confidence: " + str(instance['Confidence']))
            print()

        print ("Parents:")
        for parent in label['Parents']:
            print ("   " + parent['Name'])
        print ("----------")
        print ()
    return len(response['Labels']), countPerson, coordinate

#main 함수
def lambda_handler(event, context):
    #이벤트(지정한 S3에 새 이미지 업로드)발생 시 발생 패킷, 업로드 이미지명을 가져온다.
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    
    #파일명, 버킷명을 입력하고 rekognition 호출
    #리턴값은 라벨 갯수, 사람(person) 수, 검출 좌표 저장 리스트
    label_count, person_count, coordinate = detect_labels(key, bucket)
    
    #검출 라벨 갯수 출력 (기본 코드)
    print("Labels detected: " + str(label_count))
    
    
    #이미지 이름에서 날짜, 시간 부분 문자열 가져오기
    imageDateTime = key
    imageDate = key[0:8]
    imageHour = key[9:11]
    imageminute = key[11:13]
    
    #결과값 출력
    print("person_count : ", person_count)
    print("image Date, Hour, minute : ", imageDate, imageHour, imageminute)
    print("coordinate : ", coordinate)
    try:
        send_RDS(conn, person_count, imageDateTime, coordinate)
    except:
        conn = connect_RDS()
        send_RDS(conn, person_count, imageDateTime, coordinate)



    
    #변수에 저장된 내용들▼
    #key = 파일 명
    #person_count = 이미지에서 검출된 인원 수 !
    #imageDate = 이미지 촬영 날짜(date YYYYMMDD)
    #imageHour = 이미지 촬영 시간(hour)
    #imageminute = 이미지 촬영 분(min)
    #coordinate = 좌표값 저장한 리스트 [[top, left, width, height], [top, left, width, height], ...]
    
    #장소 정보는 테이블 명 달리해서 전송
    #시간, 인원수, 장소 순