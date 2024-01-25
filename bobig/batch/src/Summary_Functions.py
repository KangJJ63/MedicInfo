'''
1. 파일이름 : Summary_Functions
2. 기능 : 데이터 현황 등록
3. 처리절차 : 데이터 현황 등록
    1) 배치 상태코드 (54,84 등) 매개변수 받음
    2) 데이터 파일 조회
    3) 해당 데이터 파일 정보(최종 수정일자, 데이터 갯수 등) 입력
    4) 작업결과 저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.10.5 배포
    1.01 2020.10.6 소스디렉토리 조정
'''

import os
from datetime import datetime
import sys
import logging
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.base_bobig import *
from base.tibero_dbconn import *
from base.query_sep import *
import csv

#전역변수 설정
conn = tibero_db_conn()
cur = conn.cursor()
BT_SEQ = 0
BT_EXEC_SEQ = 0
WK_DTL_TP_CD = ''
CNORGCODE = 0
class custom_checkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

'''
1. 기능 : TBPBTV001(배치등록) 대상 조회
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : cur , 배치등록 상태코드
4. return : 조회 대상 쿼리 결과
'''
def TBPBTV001_sel(file_pre_name):
    global cur, BT_SEQ , WK_DTL_TP_CD
    tbpbtv001_target_sel_01_src = SQL_DIR + '/' + 'TBPBTV001_SEL_01.sql'
    tbpbtv001_target_sel_01 \
        = query_seperator(tbpbtv001_target_sel_01_src).format(wk_dtl_tp_cd = WK_DTL_TP_CD,
                                                              bt_seq = BT_SEQ,
                                                              crt_pgm_id = file_pre_name
                                                              )
    cur.execute(tbpbtv001_target_sel_01)
    target_sel= cur.fetchall()
    return target_sel


'''
1. 기능 : TBPBTV002(배치결과내역) 최초 등록
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : cur , 배치등록번호 , 실행 파일명
4. return : 배치결과 등록번호 
'''
def TBPBTV002_ins(file_nm):
    global conn,cur,BT_SEQ
    TBPBTV002_ins_01_src = SQL_DIR + '/' + 'TBPBTV002_ins_01.sql'
    TBPBTV002_ins_01 = query_seperator(TBPBTV002_ins_01_src).format(bt_seq=BT_SEQ,
                                                                    crt_pgm_id=file_nm)

    cur.execute(TBPBTV002_ins_01)
    conn.commit()
    logging.info('TBPBTV002 ins 완료. \n {}'.format(TBPBTV002_ins_01))
    # BT_EXEC_SEQ 확인
    TBPBTV002_sel_02_src = SQL_DIR + '/' + 'TBPBTV002_sel_01.sql'
    TBPBTV002_sel_02 = query_seperator(TBPBTV002_sel_02_src).format(bt_seq=BT_SEQ)

    cur.execute(TBPBTV002_sel_02)
    batch_sel_rec = cur.fetchone()
    return int(batch_sel_rec[0])


'''
1. 기능 : TBPINB103(연계데이터 파일내역) 조회
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : cur, oid, 신청id, 가설id
4. return : 조회 대상 쿼리 결과
'''
def TBPINB103_sel(ask_id,rshp_id):
    global cur
    TBPINB103_sel_01_src =  SQL_DIR + '/' + 'TBPINB103_SEL_01.sql'
    TBPINB103_sel_01 = query_seperator(TBPINB103_sel_01_src).format(ask_id = ask_id,
                                                                    rshp_id = rshp_id)
    cur.execute(TBPINB103_sel_01)
    TBPINB103_sel_file = cur.fetchall()
    return TBPINB103_sel_file


'''
1. 기능 : 상태코드별 조회 파일명 변환
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : 기존 연계데이터 파일명 , 배치등록상태코드 , 기관코드,실행번호 , 데이터셋
4. return : 변환된 파일명 
'''
def FILE_Naming(FILE_TEMP_NM,CAT_CD,EXEC_SEQ):
    global CNORGCODE,WK_DTL_TP_CD
    FILE_SPLIT_NM = FILE_TEMP_NM.split('_')
    if WK_DTL_TP_CD == '54':
        FILE_TOT_NM = FILE_TEMP_NM

    elif WK_DTL_TP_CD == '84':
        if CNORGCODE == 'K0001':
            FILE_SPLIT_NM[2] = '801'
        else :
            FILE_SPLIT_NM[2] = '802'

        # 맨 마지막 날짜가 길경우 줄여서 쓰기
        if len(FILE_SPLIT_NM[-1]) > 12:
            FILE_SPLIT_NM[-1] = FILE_SPLIT_NM[-1][:8] + '.txt'

        # 데이터셋명 재정의
        if len(FILE_SPLIT_NM[5:-3]) > 1:
            for i in range(len(FILE_SPLIT_NM[5:-3]) - 1):
                FILE_SPLIT_NM.remove(FILE_SPLIT_NM[5])
        FILE_SPLIT_NM[5] = CAT_CD

        FILE_TOT_NM = "_".join(FILE_SPLIT_NM)

    elif WK_DTL_TP_CD == '7D':
        FILE_SPLIT_NM.insert(-3, 'S')
        FILE_TOT_NM = '_'.join(FILE_SPLIT_NM)

    elif WK_DTL_TP_CD == '7E':
        if CNORGCODE == 'K0001':
            FILE_SPLIT_NM[2] = '801'
            FILE_SPLIT_NM.insert(-3, str(EXEC_SEQ))
            FILE_SPLIT_NM.insert(-4, 'S')
            FILE_TOT_NM = '_'.join(FILE_SPLIT_NM)
        else :
            FILE_SPLIT_NM[2] = '802'
            FILE_SPLIT_NM.insert(-3, str(EXEC_SEQ))
            FILE_SPLIT_NM.insert(-4, 'S')
            FILE_TOT_NM = '_'.join(FILE_SPLIT_NM)
    return FILE_TOT_NM



'''
1. 기능 : TBPINV105(기관데이터 파일내역) 파일 상태 등록
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : conn, cur, 배치상태코드, 신청id, 가설id, 기관코드, 데이터셋, 페이지, 총 페이지, 파일 날짜, 파일 크기, 파일행 수,
            파일 컬럼수, 실행 대상 프로그램명
'''
def TBPINV105_ins(ask_id,rshp_id,prvdr_cd,cat_cd,
                  file_size,file_rows_size,file_columns_size,file_nm,crt_pgm_id):
    conn1 = tibero_db_conn()
    cur1 = conn1.cursor()
    if WK_DTL_TP_CD == '54':
        org_mrg_tp_cd = '1'
    else :
        org_mrg_tp_cd = '2'

    file_split_nm = file_nm.split('_')
    prsnt_pg_nm = file_split_nm[-3]
    tot_pg_nm = file_split_nm[-2]
    local_file_ymd = file_split_nm[-1][:8]
    TBPINV105_ins_01_src = SQL_DIR + '/' + 'TBPINV105_ins_01.sql'
    TBPINV105_ins_01 = query_seperator(TBPINV105_ins_01_src).format(ask_id = ask_id,
                                                                    rshp_id = rshp_id,
                                                                    prvdr_cd = prvdr_cd,
                                                                    cat_cd = cat_cd,
                                                                    org_mrg_tp_cd = org_mrg_tp_cd,
                                                                    prsnt_pg_nm = prsnt_pg_nm,
                                                                    tot_pg_nm = tot_pg_nm,
                                                                    local_file_ymd = local_file_ymd,
                                                                    file_size = file_size,
                                                                    file_rows_size = file_rows_size,
                                                                    file_columns_size = file_columns_size,
                                                                    file_nm = file_nm,
                                                                    crt_pgm_id = crt_pgm_id)
    cur1.execute(TBPINV105_ins_01)
    conn1.commit()



'''
1. 기능 : TBPINV106(비식별데이터 파일내역) 파일 상태 등록
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : conn, cur, 배치상태코드, 신청id, 가설id, 기관코드, 데이터셋, 페이지, 총 페이지, 파일 날짜, 파일 크기, 파일행 수,
            파일 컬럼수, 실행 대상 프로그램명
'''
def TBPINV106_ins(ask_id,rshp_id,prvdr_cd,cat_cd,exec_seq,
                  file_size,file_rows_size,file_columns_size,file_nm,crt_pgm_id):
    #global conn,cur
    conn1 = tibero_db_conn()
    cur1 = conn1.cursor()

    if WK_DTL_TP_CD == '84':
        org_mrg_tp_cd = '1'
    else :
        org_mrg_tp_cd = '2'

    file_split_nm = file_nm.split('_')
    prsnt_pg_nm = file_split_nm[-3]
    tot_pg_nm = file_split_nm[-2]
    local_file_ymd = file_split_nm[-1][:8]

    TBPINV106_ins_01_src = SQL_DIR + '/' + 'TBPINV106_ins_01.sql'
    TBPINV106_ins_01 = query_seperator(TBPINV106_ins_01_src).format(ask_id = ask_id,
                                                                    rshp_id = rshp_id,
                                                                    prvdr_cd = prvdr_cd,
                                                                    cat_cd = cat_cd,
                                                                    org_mrg_tp_cd=org_mrg_tp_cd,
                                                                    prsnt_pg_nm = prsnt_pg_nm,
                                                                    tot_pg_nm = tot_pg_nm,
                                                                    exec_seq = exec_seq,
                                                                    local_file_ymd = local_file_ymd,
                                                                    file_size = file_size,
                                                                    file_rows_size = file_rows_size,
                                                                    file_columns_size = file_columns_size,
                                                                    file_nm = file_nm,
                                                                    crt_pgm_id = crt_pgm_id)
    # cur.execute(TBPINV106_ins_01)
    # conn.commit()
    cur1.execute(TBPINV106_ins_01)
    conn1.commit()



'''
1. 기능 : TBPBTV003(배치상세결과내역) 등록
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : conn, 배치결과 실행번호, 배치등록 실행번호, 배치등록상태코드, 배치결과, 실행 파일명
4. return : 조회 대상 쿼리 결과
'''
def TBPBTV003_ins(wk_exec_sts_cd, wk_exec_cnts,file_nm):
    global BT_SEQ,conn,cur,BT_EXEC_SEQ
    TBPBTV003_ins_01_src = SQL_DIR + '/' + 'TBPBTV003_ins_01.sql'
    TBPBTV003_ins_01 = query_seperator(TBPBTV003_ins_01_src).format(bt_exec_seq=BT_EXEC_SEQ,
            bt_seq=BT_SEQ,
            wk_exec_sts_cd=wk_exec_sts_cd,
            wk_exec_cnts=wk_exec_cnts,
            crt_pgm_id=file_nm)
    logging.info('TBPBTV003_ins_01 {} == {}'.format(TBPBTV003_ins_01,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV003_ins_01)
    conn.commit()

def TBPBTV003_sel(ask_id,rshp_id):
    global BT_SEQ,cur,WK_DTL_TP_CD,BT_EXEC_SEQ
    TBPBTV003_sel_01_src = SQL_DIR + '/' + 'TBPBTV003_sel_01.sql'
    TBPBTV003_sel_01 = query_seperator(TBPBTV003_sel_01_src).format(bt_seq=BT_SEQ,
                                                                    bt_exec_seq=BT_EXEC_SEQ)
    cur.execute(TBPBTV003_sel_01)
    TBPBTV003_sel_01_fetchall = cur.fetchall()

    fail_cnt = 0
    warn_cnt = 0

    # 902개수/ 003 개수 체크
    for sys_cd in TBPBTV003_sel_01_fetchall:
        if sys_cd[0] == '902':
            fail_cnt = sys_cd[1]
        elif sys_cd[0] == '003':
            warn_cnt = sys_cd[1]


    if fail_cnt == 0:
        if warn_cnt != 0 :
            TBPBTV002_upt('003', '파이썬 작업 성공', '현황 작업 완료')
        else:
            TBPBTV002_upt('002', '파이썬 작업 성공', '현황 작업 완료')

        #기관현황인 경우 신청정보 상태코드를 440으로 바꾸기
        if WK_DTL_TP_CD == '54':
            T_BDREQUESTINFO_upt_01_src = SQL_DIR + '/' + 'T_BDREQUESTINFO_upt_01.sql'
            T_BDREQUESTINFO_upt_01 = query_seperator(T_BDREQUESTINFO_upt_01_src).format(ask_id=ask_id,
                                                                                        rshp_id=rshp_id)
            logging.info('T_BDREQUESTINFO_upt_01 {} == {}'.format(T_BDREQUESTINFO_upt_01,
                                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            cur.execute(T_BDREQUESTINFO_upt_01)
            conn.commit()
    else:
        TBPBTV002_upt('902', '파이썬 작업 실패', '현황 실패.현황 작업 실패 파일 개수 = {}개'.format(fail_cnt))




'''
1. 기능 : TBPBTV002(배치상세결과내역) 작업 결과 갱신 
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : conn, 상태 코드 ,성공 여부 , 에러 구문, 배치결과등록번호
'''
# 8. TBPBTV003 총 실패여부에 따른 TBPBTV002 결과내역 업데이트 함수
def TBPBTV002_upt(wk_sts_cd, wk_rslt_cnts, wk_rslt_dtl_cnts):
    global BT_SEQ,cur,conn,BT_EXEC_SEQ
    TBPBTV002_upt_01_src = SQL_DIR + '/' + 'TBPBTV002_upt_01.sql'
    TBPBTV002_upt_01 = query_seperator(TBPBTV002_upt_01_src).format(wk_sts_cd=wk_sts_cd,
                                                                    wk_rslt_cnts=wk_rslt_cnts,
                                                                    wk_rslt_dtl_cnts=wk_rslt_dtl_cnts,
                                                                    bt_exec_seq=BT_EXEC_SEQ,
                                                                    bt_seq = BT_SEQ
                                                                    )
    logging.info('TBPBTV002_upt_01 {} == {}'.format(TBPBTV002_upt_01,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV002_upt_01)
    conn.commit()



'''
1. 기능 : TBPBTV114() 대상 조회
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : cur , 신청id, 가설id, 기관코드, 데이터셋, 실행번호, 상태코드, 작업메세지, 실행파일명
'''
def TBPINV114_ins(ask_id,rshp_id,prvdr_cd,cat_cd,exec_seq,task_name,file_nm):
    global WK_DTL_TP_CD,cur
    TBPINV114_INS_01_src = SQL_DIR + '/' + 'TBPINV114_INS_01.sql'
    TBPINV114_INS_01 = \
        query_seperator(TBPINV114_INS_01_src).format(ask_id=ask_id,
                                                     rshp_id=rshp_id,
                                                     prvdr_cd=prvdr_cd,
                                                     cat_cd=cat_cd,
                                                     exec_seq=exec_seq,
                                                     di_wk_div_cnts=WK_DTL_TP_CD,
                                                     wk_exec_cnts=task_name,
                                                     crt_pgm_id=file_nm)
    logging.info('TBPINV114_INS_01 {} == {}'.format(TBPINV114_INS_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINV114_INS_01)

#파일 행길이 함수
def row_len(path,file_nm):
    with open(path+'/'+file_nm,'r', encoding='utf-8') as data:
        reader = csv.reader(data)
        first_col = next(zip(*reader))
    return len(first_col)-1

#파일 컬럼수 함수
def col_len(path,file_nm):
    with open(path+'/'+file_nm,'r',encoding="UTF-8") as data:
        columns = data.readlines(1)
        file_col = columns[0][:-1]

    file_col = file_col.split(',')

    return len(file_col)

'''
1. 기능 : 현황 작업 로직
2. 최종수정자 : 강재전 2020.10.28
3. 파라미터 : 호출 파일명, 배치등록상태코드
'''
def Summary_Logic(file_pre_name,wk_dtl_tp_cd,string):
    global conn,cur,BT_SEQ,WK_DTL_TP_CD,CNORGCODE,BT_EXEC_SEQ


    BT_SEQ = int(string)
    WK_DTL_TP_CD = wk_dtl_tp_cd
    logging.basicConfig(
        filename=LOG_DIR + '/' + file_pre_name + '_' + datetime.now().strftime('%Y%m%d') + '.log', \
        level=eval(LOG_LEVEL), filemode='w', \
        format='%(levelname)s : line = %(lineno)d , message = %(message)s')


    logging.info('Summary program start time == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    logging.info('recv string = {}'.format(string))
    # 배치 상태 코드가 wk_dtl_tp_cd인 대상 조회
    TBPBTV001_sel_01_fetchall = TBPBTV001_sel(file_pre_name)

    # 대상이 있으면
    if len(TBPBTV001_sel_01_fetchall) != 0:

        logging.info('{} 배치등록내역이 존재함'.format(WK_DTL_TP_CD))
        BT_val = TBPBTV001_sel_01_fetchall[0]
        try:
            ask_id = BT_val[0]
            # if ask_id[:4] == '2018':
            #     err_msg = 'ASK_ID = {} => 18년도 과제이므로 생략합니다.'.format(ask_id)
            #     raise custom_checkError(err_msg)
            rshp_id = BT_val[1]
            #BT_SEQ = int(BT_val[3])
            BT_EXEC_SEQ = TBPBTV002_ins(file_pre_name)
            if BT_val[2] is None:
                err_msg = '배치등록정보에 실행일련번호(EXEC_SEQ)가 없습니다.'
                raise custom_checkError(err_msg)

            exec_seq = int(BT_val[2])
            CNORGCODE = BT_val[3]
            SRC_PTH_NM = BT_val[5]

            target_file = TBPINB103_sel(ask_id, rshp_id)
            logging.info('TBPINB103_sel 개수 = {} '.format(len(target_file)))
            if len(target_file) == 0:
                err_msg = 'TBPINB103 내역이 존재하지않습니다.'
                raise custom_checkError(err_msg)

            for meta_val in target_file:
                prvdr_cd = meta_val[0]
                cat_cd = meta_val[1]
                file_temp_nm = meta_val[2]

                FILE_NM = FILE_Naming(file_temp_nm,cat_cd,exec_seq)
                logging.info('파일체크 시작. 파일이름 = {}'.format(FILE_NM))

                #현황신청 코드별 파일 경로 설정
                if WK_DTL_TP_CD == '54':
                    if meta_val[3] != 'S':
                        err_msg = 'TBPINB103의 {} 상태코드(PRCS_FAIL_YN) 에러'.format(file_temp_nm)
                        raise custom_checkError(err_msg)
                    if SRC_PTH_NM is not None:
                        FILE_PATH = SRC_PTH_NM
                    else:
                        FILE_PATH = Bobig_ori_prov_path()
                elif WK_DTL_TP_CD == '84':
                    if SRC_PTH_NM is not None:
                        FILE_PATH = SRC_PTH_NM
                    else:
                        FILE_PATH = Bobig_deid_path(CNORGCODE)


                if os.path.isfile(FILE_PATH + '/' + FILE_NM):
                    logging.info('파일이 존재합니다. file_name == ' + FILE_NM)

                    # 파일사이즈
                    FILE_SIZE = str(os.path.getsize(filename=FILE_PATH+'/'+FILE_NM))
                    # 행길이
                    FILE_ROWS_SIZE = row_len(FILE_PATH,FILE_NM)
                    # 컬럼길이
                    FILE_COLUMNS_SIZE = col_len(FILE_PATH,FILE_NM)

                    print(FILE_SIZE,FILE_ROWS_SIZE,FILE_COLUMNS_SIZE)
                    logging.info(
                        'ASK_ID = {} , RSHP_ID = {} , PRVDR_CD = {} , CAT_CD = {} '.format(ask_id,
                                                                                           rshp_id,
                                                                                           prvdr_cd,
                                                                                           cat_cd))
                    if WK_DTL_TP_CD in ('54', '7D'):
                        TBPINV105_ins( ask_id, rshp_id, prvdr_cd, cat_cd,
                                      FILE_SIZE, FILE_ROWS_SIZE, FILE_COLUMNS_SIZE,
                                      FILE_NM, file_pre_name)

                    else:
                        TBPINV106_ins( ask_id, rshp_id, prvdr_cd, cat_cd,
                                      exec_seq,FILE_SIZE, FILE_ROWS_SIZE, FILE_COLUMNS_SIZE,
                                      FILE_NM, file_pre_name)
                    msg = '배치 상태 {} / {} 파일 현황등록 완료'.format(WK_DTL_TP_CD,FILE_NM)
                    TBPBTV003_ins('002', msg, file_pre_name)
                    logging.info(msg)
                    print(msg)
                    #TBPINV114_ins(ask_id, rshp_id, prvdr_cd, cat_cd, exec_seq,' 현황 완료',file_pre_name)


                else:
                    err_msg = '{}이 존재하지 않습니다.'.format(FILE_NM)
                    print(err_msg)
                    logging.info(err_msg)
                    TBPBTV003_ins('902',err_msg,file_pre_name)
                    #TBPINV114_ins(ask_id, rshp_id, prvdr_cd, cat_cd, exec_seq, err_msg, file_pre_name)



            TBPBTV003_sel(ask_id,rshp_id)

        except custom_checkError as e:
            err_msg = e.msg
            TBPBTV003_ins('902',err_msg,file_pre_name)
            TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)
            logging.info(err_msg)
            print(err_msg)

        except:
            err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
            TBPBTV003_ins('902', err_msg, file_pre_name)
            TBPBTV002_upt('902','파이썬 작업 실패',err_msg)
            err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
            print(err_line,err_msg)
            # logging.error(err_line, '  err_msg  ', err_msg)

    else:
        logging.info('TBPBTV001 대상이 없습니다.  == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    cur.close()
    conn.close()

