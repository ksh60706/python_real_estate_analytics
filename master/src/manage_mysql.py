# ===============================================================================
# Program Name          :       manage_mysql.py
# Date                  :       12-Aug-2020
# Version               :       1.0
# Author                :       SOO-HWAN, KIM (SH)
# Description           :       MySQL 관리 모듈
# REFERENCE
# ===============================================================================
#
#
# ===============================================================================
#
# Revision History
# ===============================================================================
# Author            Date            Version  Change  Description
# ===============================================================================
# SH                12-Aug-2020     1.0      INIT
#
#
# ===============================================================================

import pymysql

class MySQLMngt:

    @classmethod
    def conn_mysql(cls):
        '''
        MySQL 데이터베이스 연결
        :return: conn 커넥션
        '''
        conn = pymysql.connect(
            #host='192.168.245.237',
            host='localhost',
            user='id_estate',
            password='id_estate00',
            db='real_estate',
            charset='utf8'
        )

        return conn

    @classmethod
    def select_mysql(cls, conn, query):
        '''
        MySQL 데이터베이스 select 쿼리
        :param conn: 커넥션
        :param query: 쿼리
        :return: 쿼리 결과
        '''
        cursor = conn.cursor()
        cursor.execute(query)
        return cursor.fetchall()