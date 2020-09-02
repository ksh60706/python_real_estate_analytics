# =====================================================================================
# Program Name          :       manage_csv.py
# Date                  :       10-Aug-2020
# Version               :       1.1
# Author                :       SOO-HWAN, KIM (SH)
# Description           :       CSV 관리 모듈
# REFERENCE
# =====================================================================================
#
#
# =====================================================================================
#
# Revision History
# =====================================================================================
# Author            Date            Version  Change  Description
# =====================================================================================
# SH                10-Aug-2020     1.0      INIT
# SH                02-Sep-2020     1.1      ADD     csv 데이터 읽기 (read_multi_row)추가
#
# =====================================================================================

import os
import csv

class CSVMngt:

    @classmethod
    def make_csv_file(cls, filename, fieldnames):
        '''
        csv 파일이 존재하는지 체크하고 없으면 생성
        :param filename: 파일명
        :param fieldnames: 필드명
        :return:
        '''
        try:
            if not os.path.isfile(filename):
                with open(filename, "w", newline="") as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
        except Exception as ex:
            print(ex)

    @classmethod
    def write_one_row(cls, filename, fieldnames, row):
        '''
        csv 파일에 데이터 추가 (1행)
        :param filename: 파일명
        :param fieldnames: 필드명
        :param row: 데이터 1줄
        :return:
        '''
        try:
            with open(filename, "a", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerow(row)
        except Exception as ex:
            print(ex)

    @classmethod
    def write_multi_row(cls, filename, fieldnames, rows):
        '''
        csv 파일에 데이터 추가 (다중 행)
        :param filename: 파일명
        :param fieldnames: 필드명
        :param rows: 데이터 다중
        :return:
        '''
        try:
            # make_csv_file(cls, filename, fieldnames)
            with open(filename, "a", newline="") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writerows(rows)
        except Exception as ex:
            print(ex)

    @classmethod
    def read_multi_row(cls, filename):
        '''
        csv 파일 데이터 읽기
        :param filename: 파일명
        :return: 데이터
        '''
        try:
            with open(filename, "r", encoding='utf-8') as csvfile:
                return csv.reader(csvfile)
        except Exception as ex:
            print(ex)