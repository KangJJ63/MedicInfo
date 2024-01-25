'''
1. 파일이름 : ConSummary01
2. 기능 : 기관데이터을 비식별기준에 따라 원본요약, 비식별요약을 만든다
3. 처리절차 :  기관요약 및 비식별 요약 파일 생성후 현황등록
    1) 상세작업구분(7F:기관&비식별 요약) 배치(TBPBTV001)등록 성공한 배치실행(TBPBTV002) 없음
    2) 데이터셋찾기 
    3) 데이터파일찾기 
    4) 비식별대상 메타데이터생성  : 원본에서만 생성
    5) 요약파일생성, 요약데이터저장
    6) TBPPDS, TBPDIS 데이터 등록
    7) 기관,비식별 요약현황 등록
    8) 작업결과저장
4. 최종수정 : 강재전 2020.12.23
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.10.5 배포
    1.01 2020.10.6 소스디렉토리 조정
'''

import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
import pandas as pd
from base.base_bobig import *
from base.tibero_dbconn import *
from base.query_sep import *
import logging
from datetime import datetime
import numpy as np
import time, threading
import gc
import bin.Summary_Functions as SF
import zipfile

class custom_checkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


'''
1. 기능 : TBPBTV001 배치 상태코드 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 
4. retrun : select 쿼리 결과
'''
def TBPBTV001_SEL():
    global WK_DTL_TP_CD,BT_SEQ
    TBPBTV001_SEL_02_src = SQL_DIR + '/' + 'TBPBTV001_SEL_01.sql'
    TBPBTV001_SEL_02 = query_seperator(TBPBTV001_SEL_02_src).format(wk_dtl_tp_cd=WK_DTL_TP_CD,
                                                                    bt_seq = BT_SEQ,
                                                                    crt_pgm_id = base_file_nm[0])
    logging.info('TBPBTV001_SEL_01 {} == {}'.format(TBPBTV001_SEL_02,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV001_SEL_02)
    TBPBTV001_SEL_02_fetchall = cur.fetchall()
    logging.info('TBPBTV001_SEL_01 CNT {} == {}'.format(len(TBPBTV001_SEL_02_fetchall),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return TBPBTV001_SEL_02_fetchall

'''
1. 기능 : TBPBTV002 결과 내역 최초 입력 
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 
4. retrun : 배치 결과 내역 등록번호
'''
def TBPBTV002_ins():
    global conn,cur,BT_SEQ
    # TBPBTV002 배치결과내역에 작업 시작일시 등록
    TBPBTV002_ins_01_src = SQL_DIR + '/' + 'TBPBTV002_ins_01.sql'
    TBPBTV002_ins_01 = query_seperator(TBPBTV002_ins_01_src).format(bt_seq=BT_SEQ,
                                                                    crt_pgm_id=base_file_nm[0])
    logging.info('TBPBTV002_ins_01 {} == {}'.format(TBPBTV002_ins_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV002_ins_01)
    conn.commit()

    # BT_EXEC_SEQ 확인
    TBPBTV002_sel_02_src = SQL_DIR + '/' + 'TBPBTV002_sel_01.sql'
    TBPBTV002_sel_02 = query_seperator(TBPBTV002_sel_02_src).format(bt_seq=BT_SEQ)
    logging.info('TBPBTV002_sel_02 {} == {}'.format(TBPBTV002_ins_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV002_sel_02)
    batch_sel_rec = cur.fetchone()
    return int(batch_sel_rec[0])

'''
1. 기능 : TBPINV115 데이터셋 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 
4. retrun : select 쿼리 결과
'''
def TBPINV115_sel():
    global ASK_ID, RSHP_ID, BT_SEQ, WK_DTL_TP_CD,cur,conn,EXEC_SEQ
    # 제공기관, 데이터셋 조회 => 메타 생성을 위한
    TBPINV115_sel_01_src = SQL_DIR + '/' + 'TBPINV115_sel_01.sql'
    TBPINV115_sel_01 = query_seperator(TBPINV115_sel_01_src).format(ask_id=ASK_ID,
                                                                    rshp_id=RSHP_ID,
                                                                    bt_seq=BT_SEQ,
                                                                    wk_dtl_tp_cd=WK_DTL_TP_CD,
                                                                    exec_seq = EXEC_SEQ)
    logging.info('TBPINV115_sel_01 {} == {}'.format(TBPINV115_sel_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINV115_sel_01)
    return cur.fetchall()



'''
1. 기능 : TBPINM102 메타정보 merge 및 select
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 기관코드,데이터셋
4. retrun : select 쿼리 결과
'''
def TBPINM102_meta_sel(PRVDR_CD,CAT_CD):
    global ASK_ID,RSHP_ID,EXEC_SEQ,RULESETSEQ,cur,conn
    # 신청변수 정보 테이블에 비식별 대상 컬럼 정보 merge : 원본
    # logging.info('SumJob_to_merge start time == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    TBPINM102_merge_01_src = SQL_DIR + '/' + 'TBPINM102_merge_01.sql'
    TBPINM102_merge_01 = query_seperator(TBPINM102_merge_01_src).format(ask_id=ASK_ID,
                                                                        rshp_id=RSHP_ID,
                                                                        prvdr_cd=PRVDR_CD,
                                                                        cat_cd=CAT_CD,
                                                                        crt_pgm_id=base_file_nm[0])
    logging.info('TBPINM102_merge_01 {} == {}'.format(TBPINM102_merge_01,
                                                      datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINM102_merge_01)
    conn.commit()

    # 신청변수 정보 테이블에서 비식별화 대상의 메타정보 추출

    #RULESETSEQ가 NULL이면 EXEC_SEQ로, 아닌경우면 TBPINV112(비식별변수) 조회시 EXEC_SEQ를 RULESETEQ로 조회
    if RULESETSEQ is not None:
        msg = 'RULESETSEQ 존재 => {}'.format(RULESETSEQ)
        logging.info(msg)
        print(msg)
        exec_sel_num = RULESETSEQ
    else:
        exec_sel_num = EXEC_SEQ

    TBPINM102_meta_sel_01_src = SQL_DIR + '/' + 'TBPINM102_meta_sel_01.sql'
    TBPINM102_meta_sel_01 = query_seperator(TBPINM102_meta_sel_01_src).format(exec_seq=exec_sel_num,
                                                                              ask_id=ASK_ID,
                                                                              prvdr_cd=PRVDR_CD,
                                                                              rshp_id=RSHP_ID,
                                                                              cat_cd=CAT_CD)
    logging.info('TBPINM102_meta_sel_01 {} == {}'.format(TBPINM102_meta_sel_01,
                                                         datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINM102_meta_sel_01)
    TBPINM102_meta_sel_01_fetchall = cur.fetchall()
    logging.info('TBPINM102_meta_sel_01 CNT {} == {}'.format(len(TBPINM102_meta_sel_01_fetchall),
                                                             datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return TBPINM102_meta_sel_01_fetchall


'''
1. 기능 : TBPINB103 연계데이터 파일내역 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 기관코드,데이터셋
4. retrun : 파일 내역 조회 결과
'''
# 5. 연계데이터 추출 결과파일 대상 셀렉 함수
def TBPINB103_sel(PRVDR_CD,CAT_CD) :
    global ASK_ID,RSHP_ID,BT_SEQ,WK_DTL_TP_CD,cur,conn
    TBPINB103_SEL_02_src = SQL_DIR + '/' + 'TBPINB103_SEL_02.sql'
    TBPINB103_SEL_02 = query_seperator(TBPINB103_SEL_02_src).format(ask_id=ASK_ID,
                                                                    rshp_id=RSHP_ID,
                                                                    prvdr_cd=PRVDR_CD,
                                                                    cat_cd=CAT_CD)
    logging.info('TBPINB103_SEL_01 {} == {}'.format(TBPINB103_SEL_02,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINB103_SEL_02)
    TBPINB103_SEL_02_fetchall = cur.fetchall()
    logging.info('TBPINB103_SEL_01 CNT {} == {}'.format(len(TBPINB103_SEL_02_fetchall),
                                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return TBPINB103_SEL_02_fetchall


'''
1. 기능 : TBPBTV003(배치상세결과내역) 저장
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 상태코드 , 메세지
'''
# 6. 배치상세내역 입력 함수
def TBPBTV003_ins(wk_exec_sts_cd,wk_exec_cnts):
    global BT_SEQ,BT_EXEC_SEQ,conn,cur
    TBPBTV003_ins_01_src = SQL_DIR + '/' + 'TBPBTV003_ins_01.sql'
    TBPBTV003_ins_01 = query_seperator(TBPBTV003_ins_01_src).format(bt_exec_seq=BT_EXEC_SEQ,
            bt_seq=BT_SEQ,
            wk_exec_sts_cd=wk_exec_sts_cd,
            wk_exec_cnts=wk_exec_cnts,
            crt_pgm_id=base_file_nm[0])
    logging.info('TBPBTV003_ins_01 {} == {}'.format(TBPBTV003_ins_01,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV003_ins_01)
    conn.commit()

'''
1. 기능 : TBPBTV002 상세 내역을 입력하기 위해 기관별 데이터셋 조회
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 :
4. return : 기관별 데이터셋
'''
def dataset_list_msg():
    global ASK_ID,RSHP_ID,EXEC_SEQ

    dataset_list = []
    TBPINB103_datalist_01_src = SQL_DIR + '/' + 'TBPINV115_datalist_sel_01.sql'
    TBPINB103_datalist_01 = query_seperator(TBPINB103_datalist_01_src).format(ask_id = ASK_ID,
                                                                              rshp_id = RSHP_ID,
                                                                              exec_seq = EXEC_SEQ)
    cur.execute(TBPINB103_datalist_01)
    TBPINB103_datalist = cur.fetchall()

    for list in TBPINB103_datalist:
        prvdr_cd = list[0]
        cat_cd = list[1]
        dataset_list.append('{} => {}'.format(prvdr_cd,cat_cd))

    return ','.join(dataset_list)

'''
1. 기능 : 요약파일 압축파일 생성 
2. 최종수정자 : 강재전 2021.01.06
'''
def summary_zip():
    global ASK_ID,file_df

    #인터페이스명 종류 확인
    ifid_list = file_df['ifid'].unique()
    #인터페이스별 zip 압축 순환
    for if_id in ifid_list:
        # 타겟 df 임시 저장
        target_df = file_df[file_df['ifid'] == if_id]
        # 인터페이스 if_id에 해당하는 데이터셋 종류 확인
        cat_list = target_df['cat_cd'].unique()
        #데이터셋별 zip 압축 순환
        for cat in cat_list:
            ask = ''.join(ASK_ID.split('-'))
            #zip 이름 설정
            zip_nm = '_'.join([if_id,ask,'A0001',cat+'.zip'])
            #zip 패스 설정
            path = set_zip_path(if_id)

            # target_df 중에서 해당 데이터 조건
            x1 = target_df[target_df['cat_cd'] == cat]
            # target_df에서 해당 조건에 해당하는 파일명(경로까지 붙여진) 리스트 추출
            file_list = list(np.array(x1['file_nm'].tolist()))
            with zipfile.ZipFile(path+'/'+zip_nm,'w') as my_zip:
                for folder, subfolders, files in os.walk(path):
                    for f in files:
                        if f in file_list :
                            full_name = os.path.join(folder,f)
                            my_zip.write(full_name,os.path.relpath(full_name,path))
                            os.remove(full_name)
                            print('{} 압축완료!'.format(f))
                my_zip.close()


'''
1. 기능 : TBPBTV003(배치상세결과내역) 실패 여부 조회
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 메세지
'''
def TBPBTV003_sel(msg):
    global BT_SEQ,BT_EXEC_SEQ,file_df
    TBPBTV003_sel_01_src = SQL_DIR + '/' + 'TBPBTV003_sel_01.sql'
    TBPBTV003_sel_01 = query_seperator(TBPBTV003_sel_01_src).format(bt_seq=BT_SEQ,
                                                                    bt_exec_seq=BT_EXEC_SEQ)
    cur.execute(TBPBTV003_sel_01)
    TBPBTV003_sel_01_fetchall = cur.fetchall()

    fail_cnt = 0
    warn_cnt = 0

    #902개수/ 003 개수 체크
    for sys_cd in TBPBTV003_sel_01_fetchall:
        if sys_cd[0] == '902':
            fail_cnt = sys_cd[1]
        elif sys_cd[0] =='003':
            warn_cnt = sys_cd[1]

    if fail_cnt == 0:
        tot_msg = msg + dataset_list_msg()

        if warn_cnt != 0 :
            TBPBTV002_upt('003', '파이썬 작업 성공', tot_msg)
        else:
            TBPBTV002_upt('002','파이썬 작업 성공',tot_msg)
    else:
        TBPBTV002_upt('902', '파이썬 작업 실패', '비식별 작업 실패. 실패 메세지 ={}개'.format(fail_cnt))

    #만약 생성파일이 하나라도 있으면 압축하기
    if len(file_df) != 0 :
        summary_zip()

'''
1. 기능 : TBPBTV002(배치상세결과내역) 작업 결과 갱신 
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태 코드 ,성공 여부 , 에러 구문

'''
# 8. TBPBTV003 총 실패여부에 따른 TBPBTV002 결과내역 업데이트 함수
def TBPBTV002_upt(wk_sts_cd, wk_rslt_cnts, wk_rslt_dtl_cnts):
    global BT_SEQ,BT_EXEC_SEQ,conn,cur

    TBPBTV002_upt_01_src = SQL_DIR + '/' + 'TBPBTV002_upt_01.sql'
    TBPBTV002_upt_01 = query_seperator(TBPBTV002_upt_01_src).format(wk_sts_cd=wk_sts_cd,
                                                                    wk_rslt_cnts=wk_rslt_cnts,
                                                                    wk_rslt_dtl_cnts=wk_rslt_dtl_cnts,
                                                                    bt_exec_seq=BT_EXEC_SEQ,
                                                                    bt_seq = BT_SEQ)
    logging.info('TBPBTV002_upt_01 {} == {}'.format(TBPBTV002_upt_01,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV002_upt_01)
    conn.commit()


'''
1. 기능 : TBPINV112 비식별 기준을 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 기관코드,데이터셋명
4. return : 요약변수 비식별 조회 결과
'''
def TBPINV112_sel(PRVDR_CD,CAT_CD):
    global ASK_ID,RSHP_ID,EXEC_SEQ,RULESETSEQ,conn,cur

    if RULESETSEQ is not None :
        ex_num = RULESETSEQ
    else:
        ex_num = EXEC_SEQ
    SumJob_iden_sel_01_src = SQL_DIR + '/' + 'TBPINV112_sel_02.sql'
    SumJob_iden_sel_01 = query_seperator(SumJob_iden_sel_01_src).format(ask_id=ASK_ID,
                                                                        rshp_id=RSHP_ID,
                                                                        prvdr_cd=PRVDR_CD,
                                                                        cat_cd=CAT_CD,
                                                                        exec_seq=ex_num)
    cur.execute(SumJob_iden_sel_01)
    SumJob_iden_sel_01_fetchall = cur.fetchall()

    return SumJob_iden_sel_01_fetchall


'''
1. 기능 : TBPINV114 작업 로그 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 기관코드,데이터셋,로그 메세지
'''
def TBPINV114_ins_01(PRVDR_CD, CAT_CD, wk_exec_cnts):
    global ASK_ID,RSHP_ID,EXEC_SEQ,conn,cur
    TBPINV114_ins_01_src = SQL_DIR + '/' + 'TBPINV114_INS_01.sql'
    TBPINV114_ins_01 = query_seperator(TBPINV114_ins_01_src).format(ask_id=ASK_ID,
                                                                    rshp_id=RSHP_ID,
                                                                    prvdr_cd=PRVDR_CD,
                                                                    cat_cd=CAT_CD,
                                                                    exec_seq=EXEC_SEQ,
                                                                    di_wk_div_cnts='7F',
                                                                    wk_exec_cnts=wk_exec_cnts,
                                                                    crt_pgm_id=base_file_nm[0])
    cur.execute(TBPINV114_ins_01)
    conn.commit()


'''
1. 기능 : 요약데이터 현황을 등록하기 위한 함수
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 기관코드,데이터셋,파일경로,파일명, 요약데이터 구분명
'''
def Sumjob_Sum_Total(prvdr_cd,cat_cd,path,file_nm,summary_nm):
    global ASK_ID,RSHP_ID,EXEC_SEQ
    if os.path.isfile(path + '/' + file_nm):
        msg = '==== {} , {} 현황시작===='.format(file_nm,summary_nm)
        logging.info(msg)
        print(msg)


        # 파일사이즈 (Byte 단위로)
        FILE_SIZE = str(os.path.getsize(filename=path + '/' + file_nm))
        # 행길이
        FILE_ROWS_SIZE = SF.row_len(path,file_nm)
        # 컬럼길이
        FILE_COLUMNS_SIZE = SF.col_len(path,file_nm)

        if summary_nm == '기관요약':
            SF.TBPINV105_ins(ASK_ID,RSHP_ID,prvdr_cd,cat_cd,FILE_SIZE,FILE_ROWS_SIZE,FILE_COLUMNS_SIZE,
                             file_nm,base_file_nm[0])
        elif summary_nm == '비식별요약':
            SF.TBPINV106_ins(ASK_ID, RSHP_ID, prvdr_cd, cat_cd, EXEC_SEQ,FILE_SIZE,
                             FILE_ROWS_SIZE,FILE_COLUMNS_SIZE,file_nm, base_file_nm[0])
        msg = ' {} 파일 {} 현황등록 완료'.format(file_nm,summary_nm)
        logging.info(msg)
        print(msg)
        TBPBTV003_ins('002',msg)
    else:
        err_msg = '{} 파일 {} 현황 등록 실패 => 파일이 없습니다'.format(file_nm,summary_nm)
        logging.info(err_msg)
        print(err_msg)
        TBPBTV003_ins('902',err_msg)



'''
1. 기능 : TBPPDS 테이블 truncate 및 rebuild를 위한 함수
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 신청ID의 연도
'''

def TBPPDS_work(years):
    global EXEC_SEQ

    msg = 'TBPPDS{} truncate 수행'.format(years)
    print(msg)
    logging.info(msg)

    TBPPDS_trunc_01_src = SQL_DIR + '/' + 'TBPPDS_trunc_01.sql'
    TBPPDS_trunc_01 = query_seperator(TBPPDS_trunc_01_src).format(years = years,
                                                                exec_seq = EXEC_SEQ)
    cur.execute(TBPPDS_trunc_01)

    msg = 'TBPPDS{} truncate 완료. Rebuild 수행'.format(years)
    print(msg)
    logging.info(msg)

    TBPMAT_reb_01_src = SQL_DIR + '/' + 'TBPPDS_reb_01.sql'
    TBPMAT_reb_01 = query_seperator(TBPMAT_reb_01_src).format(years = years)
    cur.execute(TBPMAT_reb_01)
    conn.commit()

    msg = 'TBPPDS{} Rebuild 완료'.format(years)
    print(msg)
    logging.info(msg)


'''
1. 기능 : TBPDIS 테이블 truncate 및 rebuild를 위한 함수
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 신청ID의 연도
'''
def TBPDIS_work(years):
    global EXEC_SEQ

    msg = 'TBPDIS{} truncate 수행'.format(years)
    print(msg)
    logging.info(msg)

    TBPPDS_trunc_01_src = SQL_DIR + '/' + 'TBPDIS_trunc_01.sql'
    TBPPDS_trunc_01 = query_seperator(TBPPDS_trunc_01_src).format(years = years,
                                                                exec_seq = EXEC_SEQ)
    cur.execute(TBPPDS_trunc_01)

    msg = 'TBPDIS{} truncate 완료. Rebuild 수행'.format(years)
    print(msg)
    logging.info(msg)

    TBPMAT_reb_01_src = SQL_DIR + '/' + 'TBPDIS_reb_01.sql'
    TBPMAT_reb_01 = query_seperator(TBPMAT_reb_01_src).format(years = years)
    cur.execute(TBPMAT_reb_01)
    conn.commit()

    msg = 'TBPDIS{} Rebuild 완료'.format(years)
    print(msg)
    logging.info(msg)



'''
1. 기능 : TBPPDS / TBPDIS 요약변수 데이터 등록을 위한 함수
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 요약데이터 구분, 기관코드,데이터셋명, 요약데이터 , 현재페이지, 전체페이지, 문자컬럼 번호,문자컬럼 이름,
            숫자컬럼 번호,숫자컬럼 이름, CNT컬럼 번호
'''
def input_suminfo(sumjob_cd,prvdr_cd,cat_cd,df,prsnt_pg_num,tot_pg_num,
                  chr_col_num,chr_col_name,int_col_num,int_col_name,cnt_col_name):
    global ASK_ID,RSHP_ID,EXEC_SEQ
    try:
        #행 번호 컬럼 데이터
        pd_rownum = pd.DataFrame(data=range(1,len(df.index)+1),columns=['ROWID']).astype(str)

        #문자값 컬럼 데이터
        chr_data = df.iloc[:,chr_col_num]

        #숫자값 컬럼 데이터
        int_data = df.iloc[:,int_col_num]

        #cnt 컬럼 데이터
        cnt_data = df['CNT'].astype(str).to_frame()

        chr_data.columns = chr_col_name
        int_data.columns = int_col_name
        cnt_data.columns = cnt_col_name

        #각 데이터와 행번호 데이터 concat
        chr_merge_data = pd.concat([pd_rownum, chr_data], axis=1)
        int_merge_data = pd.concat([pd_rownum, int_data, cnt_data], axis=1)

        #각각 melt 후 전체 종합
        chr_merge_data = pd.melt(chr_merge_data, id_vars=['ROWID']).rename(
            columns={'variable': 'VAR_DSCR_SEQ', 'value': 'chr_val'}).astype(str)

        int_merge_data = pd.melt(int_merge_data, id_vars=['ROWID']).rename(
            columns={'variable': 'VAR_DSCR_SEQ', 'value': 'int_val'}).astype(str)

        tot_merge = pd.merge(chr_merge_data, int_merge_data, how='outer',
                             on=['ROWID', 'VAR_DSCR_SEQ'])

        #컬럼순서 재정의
        tot_merge = tot_merge[['ROWID','VAR_DSCR_SEQ','chr_val','int_val']]
        #sql에 한번에 넣기 위해 투플로 저장
        tot_merge = tot_merge.fillna('').values.tolist()


        if sumjob_cd == 1:
            # TBPPDS truncate
            logging.info('TBPPDS truncate  == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            # TBPPDS_work(ASK_ID[0:4])

            TBPPDS_ins_src = SQL_DIR + '/' + 'TBPPDS_ins.sql'


            TBPPDS_ins = query_seperator(TBPPDS_ins_src).format(years = ASK_ID[0:4],
                                                                ask_id = ASK_ID,
                                                                rshp_id = RSHP_ID,
                                                                prvdr_cd = prvdr_cd,
                                                                cat_cd = cat_cd,
                                                                exec_seq = EXEC_SEQ,
                                                                prsnt_pg_num = int(prsnt_pg_num),
                                                                tot_pg_num = int(tot_pg_num),
                                                                crt_pgm_id = base_file_nm[0])

            cur.executemany(TBPPDS_ins, tot_merge)
            conn.commit()

            msg = 'TBPPDS{} , 실행일련번호 = {} 등록 완료, 총 데이터 개수 = {}개'.format(ASK_ID[0:4],EXEC_SEQ,len(tot_merge))
            print(msg)
            logging.info(msg)
        else:
            # TBPDIS truncate
            logging.info('TBPDIS truncate  == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            # TBPDIS_work(ASK_ID[0:4])
            TBPPDS_ins_src = SQL_DIR + '/' + 'TBPDIS_ins.sql'

            TBPPDS_ins = query_seperator(TBPPDS_ins_src).format(years = ASK_ID[0:4],
                                                                ask_id = ASK_ID,
                                                                rshp_id = RSHP_ID,
                                                                prvdr_cd = prvdr_cd,
                                                                cat_cd = cat_cd,
                                                                exec_seq = EXEC_SEQ,
                                                                prsnt_pg_num = int(prsnt_pg_num),
                                                                tot_pg_num = int(tot_pg_num),
                                                                crt_pgm_id = base_file_nm[0])

            cur.executemany(TBPPDS_ins, tot_merge)
            conn.commit()
            msg = 'TBPDIS{} , 실행일련번호 = {} 등록 완료, 총 데이터 개수 = {}개'.format(ASK_ID[0:4],EXEC_SEQ,len(tot_merge))
            print(msg)
            logging.info(msg)
    except:
        err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')

        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)

        print(err_line, '  err_msg=', err_msg)


'''
1. 기능 : 요약파일 파일명 변환
2. 최종수정자 : 강재전 2021.01.06
3. 파라미터 : 요약구분코드,파일명,기관코드,데이터셋명
4. return : 변환된 파일명
'''
def file_naming(code, file_nm, prvdr, cat_cd):
    global EXEC_SEQ
    file_sp = file_nm.split('_')


    file_sp.insert(-3, 'S')

    # 맨 마지막 날짜가 길경우 줄여서 쓰기
    if len(file_sp[-1]) > 12:
        file_sp[-1] = file_sp[-1][:8] + '.txt'

    # 데이터셋명 재정의
    if len(file_sp[5:-4]) > 1:
        for i in range(len(file_sp[5:-4]) - 1):
            file_sp.remove(file_sp[5])
    file_sp[5] = cat_cd

    if prvdr == 'K0001':
        if code == 1:
            file_sp[2] = '501'
        elif code == 2:
            file_sp[2] = '801'
    elif prvdr == 'K0002':
        if code == 1:
            file_sp[2] = '502'
        elif code == 2:
            file_sp[2] = '802'
    elif prvdr == 'K0003':
        file_sp[2] = '503'
    elif prvdr == 'K0004':
        file_sp[2] = '504'

    if code != 1:
        file_sp.insert(-3, str(EXEC_SEQ))

    file_new_nm = '_'.join(file_sp)

    return file_new_nm


'''
1. 기능 : 연계데이터 파일 요약 로직 수행
2. 최종수정자 : 강재전 2020.12.23
3. 파라미터 : 신청변수 컬럼, 요약변수컬럼, TBPINB103_sel 파일내역 결과, 기관 코드, 데이터셋 , 저장경로, 문자변수 컬럼 번호, 문자변수 컬럼이름,
            숫자변수 컬럼 번호, 숫자변수 컬럼이름, CNT 컬럼번호
'''
# 9. 요약 로직 함수
def Summary_file(ori_values_column,var_cd_col,meta_val,prvdr_cd, cat_cd,RSLT_PTH_NM,
                 chr_col_num,chr_col_name,int_col_num,int_col_name,CNT_COL):
    try:
        global ASK_ID,RSHP_ID,EXEC_SEQ,BT_SEQ,BT_EXEC_SEQ,CNORGCODE,conn,\
            cur,sum01_cnt,sum02_cnt,file_df

        ori_v_c_copy = ori_values_column[:]

        file_nm = meta_val[0]

        file_split_nm = file_nm.split('_')

        #현재 페이지 num
        prnt_pg_num = file_split_nm[-3]
        # 전체 페이지 num
        tot_pg_num = file_split_nm[-2]

        #원본요약 파일
        SumJob01_Summary_File_Nm = file_naming(1,file_nm,prvdr_cd,cat_cd)

        #압축을 위한 기관요약 인터페이스ID 추출
        summary01_ifid = '_'.join(SumJob01_Summary_File_Nm.split('_')[:3])

        #비식별 요약파일
        SumJob02_Summary_File_Nm = file_naming(2,file_nm,CNORGCODE,cat_cd)

        # 압축을 위한 비식별 요약인터페이스ID 추출
        summary02_ifid = '_'.join(SumJob02_Summary_File_Nm.split('_')[:3])

        # 데이터 디렉토리에 연계데이터 파일이 있는지 확인

        logging.info('파일있음 {} == {} '.format(BOBIG_PROV_IN_DIR + '/' + file_nm,
                                             datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        chunk = pd.read_csv(BOBIG_PROV_IN_DIR + '/' + file_nm, encoding='utf-8', dtype = str,chunksize=10000,iterator=True,na_filter=None)
        ori_file = pd.concat([ch for ch in chunk])

        #CNT컬럼을 추가
        ori_v_c_copy.extend(['CNT'])
        #원본파일에 CNT 추가

        ori_file['CNT']= 0

        msg = "요약파일 컬럼 => {} ".format(ori_v_c_copy)
        logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        print(msg)


        #기관데이터 요약 ###
        logging.info('기관요약 작업 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('기관요약 작업 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        #ori_file 데이터와 모든 CNT값이 0으로 된 DF
        ori_sumjob_file = ori_file[ori_v_c_copy].set_index('ASK_ID')

        # 리스트에 CNT 컬럼 삭제 (group 시 사용는 CNT 값이 들어가면 안되서 리스트에서 삭제 함)
        ori_v_c_copy.remove('CNT')

        #CNT를 제외한 컬럼들을 groupby로 묶음
        SumJob_Summary_groupby = ori_sumjob_file.groupby(ori_v_c_copy)
        # 기관요약 & 비식별 요약 저장할 위치 경로 체크
        if RSLT_PTH_NM is not None:
            bobig_sumjob01_dir = RSLT_PTH_NM
            bobig_sumjob02_dir = RSLT_PTH_NM
        else:

            bobig_sumjob01_dir = Bobig_sumjob01_path(prvdr_cd)
            bobig_sumjob02_dir = Bobig_sumjob02_path(CNORGCODE)

        # groupby값을 cnt컬럼에 삽입하고 원본데이터 요약 파일 저장

        SumJob_Summary_groupby.count()['CNT'].to_csv(bobig_sumjob01_dir + '/' + SumJob01_Summary_File_Nm)
        #생성된 기관요약파일 수 증가
        sum01_cnt+=1
        #압축하기위해 file_df에 추가 (code = 1)
        file_df = file_df.append({'ifid':summary01_ifid,'cat_cd':cat_cd,
                                  'file_nm':SumJob01_Summary_File_Nm },
                                 ignore_index=True)


        # TBPINV114에 원본데이터 요약 로그 입력
        TBPINV114_ins_01(prvdr_cd, cat_cd, SumJob01_Summary_File_Nm)
        msg='{} 기관요약 작업 완료 == '.format(file_nm) + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(msg)
        print(msg)
        TBPBTV003_ins('002', msg)

        # #TBPPDS 테이블에 저장
        logging.info('TBPPDS 요약정보 insert 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('TBPPDS 요약정보 insert 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        input_suminfo(1, prvdr_cd, cat_cd, SumJob_Summary_groupby.count()['CNT'].to_frame().reset_index()[var_cd_col],
                      prnt_pg_num, tot_pg_num, chr_col_num, chr_col_name, int_col_num, int_col_name, CNT_COL)

        logging.info('TBPPDS 요약정보 insert 완료 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('TBPPDS 요약정보 insert 완료 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        #########요약데이터
        #인덱스를 리셋
        ClassicDeId_data = ori_sumjob_file.reset_index()
        c_column = ClassicDeId_data.columns.tolist()
        logging.info('비식별 작업 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        #TBPINV112 신청변수목록 조회
        TBPINV112_col_list = TBPINV112_sel(prvdr_cd,cat_cd)

        for deid_val in TBPINV112_col_list:
            col_nm = deid_val[0]
            if deid_val[1] is None:
                continue
            else:
                if col_nm in c_column:
                    deid_val_split = deid_val[1].split('->')
                    # 변환할 타입
                    change_type = deid_val_split[0]
                    # 파이썬 문법
                    di_set_val = deid_val_split[1]

                    if change_type == 'date':
                        temp = pd.DataFrame(ClassicDeId_data[ClassicDeId_data[col_nm].notnull()][col_nm])
                        f1 = pd.to_datetime(temp[col_nm], format='%Y%m%d', errors='coerce')
                        temp[col_nm] = f1.fillna('0')
                        temp.drop(temp.loc[temp[col_nm] == '0'].index, inplace=True)

                    elif change_type != 'string':
                        if change_type == 'Int64':
                            f1 = pd.to_numeric(ClassicDeId_data[col_nm], errors='coerce').astype(np.int64)
                        else : 
                            f1 = pd.to_numeric(ClassicDeId_data[col_nm], errors='coerce').astype(np.float)
                        temp = pd.DataFrame(f1.fillna(-999))
                        # col_nm값이 -999인 것들 ( null / 다른 타입 등)
                        temp.drop(temp.loc[temp[col_nm] == -999].index, inplace=True)
                    else :
                        temp = pd.DataFrame(ClassicDeId_data[ClassicDeId_data[col_nm].notnull()][col_nm])

                    # 파이썬 문법 변수 -> 데이터프레임 형식으로 변환
                    di_set_val = str(di_set_val.replace(col_nm,
                                                        "temp['" + col_nm + "']"))
                    logging.info(di_set_val)
                    temp[col_nm] = eval(di_set_val)

                    # 비식별 값 적용
                    ClassicDeId_data.update(temp)
                else:
                    err_msg = 'TBPINV112의 등록된 컬럼 {}가(이) 데이터파일 {}에 존재하지 않습니다.'.format(col_nm,file_nm)
                    TBPINV114_ins_01(prvdr_cd, cat_cd, err_msg)
                    TBPBTV003_ins('902', err_msg)
                    continue
        logging.info('비식별 작업 종료  == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


        #비식별요약에 CNT컬럼 추가
        SumJob_subset = ClassicDeId_data.set_index('ASK_ID')
        SumJob_Summary_groupby = SumJob_subset.groupby(ori_v_c_copy)



        # 비식별 요약 파일 저장
        SumJob_Summary_groupby.count()['CNT'].to_csv(bobig_sumjob02_dir + '/' + SumJob02_Summary_File_Nm)

        # 생성된 비식별요약파일 수 증가
        sum02_cnt += 1

        # 압축하기위해 file_df에 추가 (code = 2)
        file_df = file_df.append({'ifid': summary02_ifid, 'cat_cd': cat_cd,
                                  'file_nm': SumJob02_Summary_File_Nm},
                                 ignore_index=True)


        # TBPINV114에 기관데이터 요약 로그 입력
        TBPINV114_ins_01(prvdr_cd, cat_cd, SumJob02_Summary_File_Nm)
        msg='{} 비식별요약 작업 완료 == '.format(file_nm) + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.info(msg)
        print(msg)
        TBPBTV003_ins('002', msg)

        # #TBPDIS 테이블에 저장
        logging.info('TBPDIS 요약정보 insert 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('TBPDIS 요약정보 insert 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        input_suminfo(2, prvdr_cd, cat_cd, SumJob_Summary_groupby.count()['CNT'].to_frame().reset_index()[var_cd_col],
                      prnt_pg_num, tot_pg_num, chr_col_num, chr_col_name, int_col_num, int_col_name, CNT_COL)


        logging.info('TBPDIS 요약정보 insert 완료 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        print('TBPDIS 요약정보 insert 완료 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        ######기관요약########
        if os.path.isfile(bobig_sumjob01_dir +'/' +SumJob01_Summary_File_Nm):
            Sumjob_Sum_Total(prvdr_cd, cat_cd,bobig_sumjob01_dir,SumJob01_Summary_File_Nm,'기관요약')
        ######비식별요약########
        if os.path.isfile(bobig_sumjob02_dir + '/' + SumJob02_Summary_File_Nm):
            Sumjob_Sum_Total(prvdr_cd, cat_cd,bobig_sumjob02_dir, SumJob02_Summary_File_Nm, '비식별요약')

        del [[chunk,ori_file]]
        gc.collect()
    except:
        err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
        TBPBTV003_ins('902',err_msg)
        print(err_line, '  err_msg=', err_msg)
                   
    # finally:
    #     if 'CNT' in ori_values_column:
    #           ori_values_column.remove('CNT')



def main():
    global WK_DTL_TP_CD,ASK_ID,RSHP_ID,EXEC_SEQ,BT_SEQ,BT_EXEC_SEQ,CNORGCODE,RULESETSEQ,conn,cur,\
        BOBIG_PROV_IN_DIR,origin_cnt,sum01_cnt,sum02_cnt,file_df

    logging.info('recv BT_SEQ = {}'.format(BT_SEQ))

    #배치 상태 코드가 7B 배치 조회
    BT_val = TBPBTV001_SEL()

    #상태코드가 7F인 대상이 없으면
    if len(BT_val) == 0:
        logging.info('상태코드 {}인 배치대상이 없습니다. == {}'.format(WK_DTL_TP_CD,
                                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        sys.exit(1)

    # 배치 등록 대상이 있을 경우 로직 수행
    #검색결과의 첫번째 배치목록만을 사용
    BT_val = BT_val[0]
    try:
        ASK_ID = BT_val[0]  #신청id
        # if ASK_ID[:4] == '2018':
        #     err_msg = 'ASK_ID = {} => 18년도 과제이므로 생략합니다.'.format(ASK_ID)
        #     raise custom_checkError(err_msg)

        RSHP_ID = BT_val[1] #가설id
        #OID = BT_val[2] #oid
        BT_EXEC_SEQ = TBPBTV002_ins() #배치등록 & 배치결과번호 함수
        if BT_val[2] is None:
            err_msg = '배치등록정보에 실행일련번호(EXEC_SEQ)가 없습니다.'
            raise custom_checkError(err_msg)
        EXEC_SEQ = int(BT_val[2]) #실행일련번호
        CNORGCODE = BT_val[3]
        if BT_val[4] is not None:
            RULESETSEQ = BT_val[4]
        if BT_val[5] is not None:
            BOBIG_PROV_IN_DIR = BT_val[5]
        RSLT_PTH_NM = BT_val[6]

        # 제공기관 및 데이터셋 조회
        TBPINV115_sel_01_fetchall = TBPINV115_sel()

        # 데이터셋 조회 대상이 없을 경우 TBPBTV002업데이트 및 로그 기록
        if len(TBPINV115_sel_01_fetchall) == 0:
            err_msg = '신청정보에 해당되는 데이터셋이 존재하지 않습니다. TBPINV115 확인'
            raise custom_checkError(err_msg)

        TBPPDS_work(ASK_ID[0:4])
        TBPDIS_work(ASK_ID[0:4])

        # 데이터셋이 존재할 경우 로직 수행
        for CAT_val in TBPINV115_sel_01_fetchall:
            PRVDR_CD = CAT_val[0]
            CAT_CD = CAT_val[1]

            #TBPINM102 신청변수 메타정보 셀렉
            TBPINM102_meta_sel_01_fetchall = TBPINM102_meta_sel(PRVDR_CD,CAT_CD)


            # 메타정보 조회 대상수가 0이 아닌경우
            if len(TBPINM102_meta_sel_01_fetchall) != 0:
                data = pd.DataFrame(data=TBPINM102_meta_sel_01_fetchall,
                                    columns = ['ASK_ID',
                                            'RSHP_ID',
                                            'PRVDR_CD',
                                            'CAT_CD',
                                            'DATATP',
                                            'VAR_CD',
                                            'VAR_DSCR_SEQ',
                                            'CNT_COL'])
                #data_set_askid_index = data.set_index('ASK_ID')
                data.columns = data.columns.str.strip()
                #메타정보 컬럼
                meta_col = data.columns[:3].tolist() + ['CAT_CD']
                var_cd_col = data['VAR_CD'][0].split(',')

                #요약파일의 컬럼들 리스트로 정의
                sumjob_col = meta_col + var_cd_col

                #요약정보 입력을 위해 var_cd_col에 cnt컬럼 추가
                var_cd_col.append('CNT')

                #문자 / 숫자 구분 리스트
                DATATP_list = data["DATATP"][0].split(',')

                #문자 컬럼 순서
                chr_col_num =  [xx_cnt for xx_cnt, xx_data in enumerate(DATATP_list) if xx_data == '문자']
                #숫자 컬럼 순서
                int_col_num = [xx_cnt for xx_cnt, xx_data in enumerate(DATATP_list) if xx_data == '숫자']
                #요약정보에 대입을 하기 위한 순서명
                dscr_col_num = data['VAR_DSCR_SEQ'][0].split(',')
                #문자형 컬렴 순서명
                chr_col_name = []
                #숫자형 컬럼 순서명
                int_col_name = []
                #CNT 순서 값
                CNT_COL = data['CNT_COL']

                for ori_DATATP_list_cnt, ori_DATATP_list_data in enumerate(DATATP_list):
                    if ori_DATATP_list_data == '문자':
                        chr_col_name.append(dscr_col_num[ori_DATATP_list_cnt])
                    if ori_DATATP_list_data == '숫자':
                        int_col_name.append(dscr_col_num[ori_DATATP_list_cnt])

                #연계데이터 대상이 있는지 확인
                TBPINB103_SEL_02_fetchall = TBPINB103_sel(PRVDR_CD,CAT_CD)

                if len(TBPINB103_SEL_02_fetchall) == 0:
                    msg = 'TBPINB103_SEL_01 select 대상이 없습니다.\n \
                           ASK_ID={} RSHP_ID={} PRVDR_CD={} CAT_CD={} '.format(ASK_ID, RSHP_ID, PRVDR_CD, CAT_CD, BT_SEQ)
                    logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                    TBPBTV003_ins('902', msg)
                    # 대상이 없으면 다음 데이터셋을 처리
                    continue

                # 요약 Thead
                start = time.perf_counter()

                for meta_val in TBPINB103_SEL_02_fetchall:
                    file_nm = meta_val[0]
                    fail_yn = meta_val[1]

                    #원본파일 수 증가
                    origin_cnt+=1

                    if os.path.isfile(BOBIG_PROV_IN_DIR + '/' + file_nm):
                        #쓰레드 summary함수
                        Summary_file(sumjob_col, var_cd_col,meta_val, PRVDR_CD, CAT_CD, RSLT_PTH_NM,
                                                   chr_col_num,chr_col_name,int_col_num,int_col_name,CNT_COL)

                    else:
                        msg = '연계데이터 파일이 없습니다. {}'.format(BOBIG_PROV_IN_DIR + '/' + file_nm)
                        logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                        print(msg)
                        TBPBTV003_ins('902', msg)


                finish = time.perf_counter()
                msg = "Thread 작업 종료 {} seconds \n작업정보  {}".format(round(finish-start, 2), meta_val)
                print(msg)
                logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))



            else:
                msg = '요약대상이 없어 요약파일 생성하지 않았습니다. PRVDR_CD={} CAT_CD={}. TBPINM102,TBPINV112 확인'.format(PRVDR_CD, CAT_CD)
                logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                print(msg)
                TBPBTV003_ins('003', msg)


        msg = 'DB파일수:{}개 / 생성된 기관요약 : {} , 비식별 요약 : {}개 '.format(origin_cnt, sum01_cnt,sum02_cnt)
        TBPBTV003_sel(msg)

    except custom_checkError as e:
        err_msg = e.msg
        TBPBTV003_ins('902',err_msg)
        TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)
        logging.info(err_msg)
        print(err_msg)

    except:
        err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        TBPBTV003_ins('902',err_msg)
        TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)
        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
        err_msg = '  err_msg == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        print(err_line, '  err_msg=', err_msg)



pd.options.mode.use_inf_as_na = True

base_file_nm = os.path.basename(__file__).split('.')
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0]+ '_' + datetime.now().strftime('%Y%m%d')   + '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')

logging.info('SumJob program start time == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

##################서머리#########################
conn = tibero_db_conn()
cur = conn.cursor()
WK_DTL_TP_CD = '7F'
ASK_ID = ''
RSHP_ID = ''
EXEC_SEQ = 0
BT_SEQ = 0
BT_EXEC_SEQ = 0
CNORGCODE = ''
RULESETSEQ = None
BOBIG_PROV_IN_DIR = Bobig_ori_prov_path()
#원본파일 수
origin_cnt = 0
#기관요약 파일 수
sum01_cnt = 0
#비식별 요약 파일 수
sum02_cnt = 0

#압축을 하기위한 데이터프레임 생성
file_df = pd.DataFrame(columns=('ifid','cat_cd','file_nm'))

if __name__ == "__main__":
    BT_SEQ = sys.argv[1]
    main()


cur.close()
conn.close()
