# ======================================================================================
# Program Name          :       retry_efficiency_apartment_transaction.py
# Date                  :       02-Sep-2020
# Version               :       1.0
# Author                :       SOO-HWAN, KIM (SH)
# Description           :       오피스텔 매매 신고정보 자료 에러 URL 재 다운 (국토교통부 API)
# REFERENCE
# ======================================================================================
# \reference\PDF\오피스텔_매매_신고정보_조회_기술문서(EfficiencyApartmentTransactionData).pdf
# \reference\HWP\오피스텔 매매 신고정보 조회 기술문서.hwp
# ======================================================================================
#
# Revision History
# ======================================================================================
# Author            Date            Version  Change  Description
# ======================================================================================
# SH                02-Sep-2020     1.0      INIT
#
#
# ======================================================================================

import requests
from datetime import datetime
import xml.etree.ElementTree as ET
import os

from master.src import manage_csv as ms
from master.src import manage_mysql as db



# 변수 정의
CSV_FILE_PATH = "../reference/FILE/오피스텔매매"
FIELD_NAMES = ['API_URL', '거래금액', '건축년도', '년', '단지', '법정동', '시군구', '월', '일', '전용면적', '지번', '지역코드', '층']
ERROR_LIST_TXT_PATH = os.path.dirname(os.path.abspath(os.path.dirname(__file__)))+"\\reference\ERROR\\collect_efficiency_apartment_transaction.txt"


def write_error_url(URL):
    '''
        에러발생시 URL 기록 남기기
    :param URL: API URL
    :return:
    '''
    f = open(ERROR_LIST_TXT_PATH, "a", encoding="utf-8")
    f.write(URL+"\n")
    f.close()

def read_error_url():
    '''
        에러 URL 읽고 파일 지우기
    :param URL: API URL
    :return:
    '''
    f = open(ERROR_LIST_TXT_PATH, "r", encoding="utf-8")
    urls = f.readlines()
    f.close()
    os.remove(ERROR_LIST_TXT_PATH)
    return urls

def get_content_from_url(note):
    '''
`       API를 통해 가져온 데이터 가공
    :param note: API 결과 xml
    :return:
        <거래금액>36,000</거래금액>
        <건축년도>2008</건축년도>
        <년>2020</년>
        <단지>광화문 풍림스페이스본</단지>
        <법정동> 사직동</법정동>
        <시군구>종로구</시군구>
        <월>1</월>
        <일>7</일>
        <전용면적>39.54</전용면적>
        <지번>9</지번>
        <지역코드>11110</지역코드>
        <층>8</층>
    '''

    try:
        rows = []
        ms.CSVMngt.make_csv_file(CSV_FILE_NM, FIELD_NAMES)

        for item in note.iter("item"):
            rows.append(
                {
                    "API_URL" : TARGET_URL,
                    "거래금액": int(item.findtext("거래금액").replace(",","")+"0000"),
                    "건축년도" : item.findtext("건축년도"),
                    "년": str(item.findtext("년")),
                    "단지": item.findtext("단지"),
                    "법정동": item.findtext("법정동"),
                    "시군구": str(item.findtext("시군구")),
                    "월": str(item.findtext("월")),
                    "일": str(item.findtext("일")),
                    "전용면적": item.findtext("전용면적"),
                    "지번": str(item.findtext("지번")),
                    "지역코드": item.findtext("지역코드"),
                    "층": str(item.findtext("층"))
                }
            )
        #print(rows)
        ms.CSVMngt.write_multi_row(CSV_FILE_NM, FIELD_NAMES, rows)


    except Exception as ex:
        write_error_url(TARGET_URL)
        print("에러 발생", ex)
        pass


def main():
    global TARGET_URL
    global CSV_FILE_NM

    TARGET_URLS = read_error_url()
    # print('url은', TARGET_URLS)
    for TARGET_URL in TARGET_URLS:
        TARGET_URL = TARGET_URL.strip("\n")
        i = TARGET_URL[-6:]

        CSV_FILE_NM = CSV_FILE_PATH + "_" + i + ".csv"
        print(TARGET_URL)

        try:
            response = requests.get(TARGET_URL).text

            tree = ET.ElementTree(ET.fromstring(response))
            note = tree.getroot()

            resultCode = note.findtext("header/resultCode")
            resultMsg = note.findtext("header/resultMsg")

            if resultCode == "00":
                get_content_from_url(note)
            else:
                raise Exception

        except Exception as ex:
            write_error_url(TARGET_URL)
            print("에러 발생", ex)




if __name__ == "__main__":
    main()