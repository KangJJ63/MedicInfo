
'''
1. 파일이름 : ClassicDeId
2. 기능 : 기관데이터를 비식별기준에 따라 비식별 파일을 만든다
3. 처리절차 : 비식별 작업
    1) 상세작업구분(7A:비식별 작업) 배치(TBPBTV001)등록 성공한 배치실행(TBPBTV002) 없음
    2) 데이터셋찾기
    3) 데이터파일찾기
    4) 비식별 작업 실행
    5) 비식별파일생성, 비식별 데이터저장
    6) 작업결과저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.12.18 배포
    1.01 2020.10.6 소스디렉토리 조정
'''




import logging
import pandas as pd
import sys,os
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.base_bobig import *
from base.tibero_dbconn import *
from base.query_sep import *
from datetime import datetime,timedelta
import gc
import math
import numpy as np
import chardet


class custom_checkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

class file_Error(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg


'''
1. 기능 : TBPBTV001 배치 상태코드 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태 코드 (WK_DTL_TP_CD)
4. retrun : select 쿼리 결과
'''
# 1. 배치 상태 코드가 7B 배치 조회
def TBPBTV001_SEL():
    global WK_DTL_TP_CD,BT_SEQ
    TBPBTV001_SEL_01_src = SQL_DIR + '/' + 'TBPBTV001_SEL_01.sql'
    TBPBTV001_SEL_01 = query_seperator(TBPBTV001_SEL_01_src).format(wk_dtl_tp_cd=WK_DTL_TP_CD,
                                                                    bt_seq =BT_SEQ,
                                                                    crt_pgm_id = base_file_nm[0])
    logging.info('TBPBTV001_SEL_01 {} == {}'.format(TBPBTV001_SEL_01,
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPBTV001_SEL_01)
    TBPBTV001_SEL_01_fetchall = cur.fetchall()
    logging.info('TBPBTV001_SEL_01 CNT {} == {}'.format(len(TBPBTV001_SEL_01_fetchall),
            datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return TBPBTV001_SEL_01_fetchall




'''
1. 기능 : TBPBTV002 결과 내역 최초 입력 
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 배치 등록 번호
4. retrun : 배치 결과 내역 등록번호
'''
# 2. TBPBTV002 결과내역 시작 입력 및 BT_EXEC_SEQ 반환 함수
def TBPBTV002_ins(BT_SEQ):
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
3. 파라미터 : 신청ID,가설ID,배치등록번호,배치상태코드
4. retrun : select 쿼리 결과
'''
# 3. 제공기관, 데이터셋 조회 함수
def TBPINV115_sel(ASK_ID,RSHP_ID,BT_SEQ,WK_DTL_TP_CD):
    global EXEC_SEQ
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
1. 기능 : TBPBTV003(배치상세결과내역) 저장
2. 최종수정자 : 강재전 2020.10.5
3. 파라미터 : 상태코드 , 메세지
'''
# 6. 배치상세내역 입력 함수
def TBPBTV003_ins(wk_exec_sts_cd, wk_exec_cnts):
    global BT_EXEC_SEQ,BT_SEQ
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
1. 기능 : TBPINB103 연계데이터 파일내역 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 기관코드,데이터셋
4. retrun : 파일 내역 조회 결과
'''

def TBPINB103_sel(prvdr_cd,cat_cd) :
    global ASK_ID,RSHP_ID,BT_SEQ,WK_DTL_TP_CD
    TBPINB103_SEL_01_src = SQL_DIR + '/' + 'TBPINB103_SEL_02.sql'
    TBPINB103_SEL_01 = query_seperator(TBPINB103_SEL_01_src).format(ask_id=ASK_ID,
                                                                    rshp_id=RSHP_ID,
                                                                    prvdr_cd = prvdr_cd,
                                                                    cat_cd = cat_cd)
    logging.info('TBPINB103_SEL_01 {} == {}'.format(TBPINB103_SEL_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINB103_SEL_01)
    TBPINB103_SEL_01_fetchall = cur.fetchall()
    logging.info('TBPINB103_SEL_01 CNT {} == {}'.format(len(TBPINB103_SEL_01_fetchall),
                                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    return TBPINB103_SEL_01_fetchall


'''
1. 기능 : 최종 결과 입력을 위한 dataset 조회
2. 최종수정자 : 강재전 2020.12.15
3. 파라미터 :
4. retrun : 기관별 데이터셋명
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
1. 기능 : TBPBTV003 상태코드 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 메세지
'''
def TBPBTV003_sel(msg):
    global BT_SEQ,BT_EXEC_SEQ
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



'''
1. 기능 : TBPBTV002 결과 내역 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 작업 결과 코드, 작업 결과 메세지, 작업결과 상세 메세지
'''
# 8. TBPBTV003 총 실패여부에 따른 TBPBTV002 결과내역 업데이트 함수
def TBPBTV002_upt(wk_sts_cd, wk_rslt_cnts, wk_rslt_dtl_cnts):
    global BT_SEQ,BT_EXEC_SEQ

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
1. 기능 : TBPINV114 작업 로그 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 기관코드,데이터셋,로그 메세지
'''
def TBPINV114_ins(prvdr_cd,cat_cd,wk_exec_cnts):
    global ASK_ID,RSHP_ID,EXEC_SEQ,WK_DTL_TP_CD
    TBPINV114_INS_01_src = SQL_DIR + '/' + 'TBPINV114_INS_01.sql'
    TBPINV114_INS_01 = \
        query_seperator(TBPINV114_INS_01_src).format(ask_id=ASK_ID,
                                                     rshp_id=RSHP_ID,
                                                     prvdr_cd=prvdr_cd,
                                                     cat_cd=cat_cd,
                                                     exec_seq=EXEC_SEQ,
                                                     di_wk_div_cnts=WK_DTL_TP_CD,
                                                     wk_exec_cnts=wk_exec_cnts,
                                                     crt_pgm_id=base_file_nm[0])
    logging.info('TBPINV114_INS_01 {} == {}'.format(TBPINV114_INS_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINV114_INS_01)


# '''
# 1. 기능 : 비식별 date type 컬럼과 RND 합을 구하기 위한 함수
# 2. 최종수정자 : 강재전 2020.12.01
# 3. 파라미터 :
# 4. return : 데이터 합 결과
# '''
# def encode(x, y):
#     if len(x) == 8 and y is not None and math.isnan(y) == False:
#         return ((datetime.strptime(x, '%Y%m%d')) + timedelta(days=float(y))).strftime('%Y%m%d')
#     elif x=='0':
#         return '00000000'

'''
1. 기능 : 기관데이터 컬럼 위치 파악을 위한 함수
2. 최종수정자 : 강재전 2020.12.15
3. 파라미터 : 파일 경로, 파일명
4. retrun : 컬럼명 list
'''
def col_check(path,file):
    columns = list()
    with open(path+'/'+file, encoding='utf-8') as f:
        limit = 1
        count = 0

        for line in f:
            columns = [word.upper() for word in line[:-1].split(',')]

            count += 1
            if count >= limit:
                break
    return columns


'''
1. 기능 : 신청변수를 조회하기 위한 함수
2. 최종수정자 : 강재전 2020.12.15
3. 파라미터 : 기관코드,데이터셋명
4. retrun : TBPINM102 조회 결과
'''
def get_columns(prvdr_cd,cat_cd):
    global ASK_ID,RSHP_ID
    col_sel_src = SQL_DIR +'/' + 'TBPINM102_col_sel_01.sql'
    col_sel = query_seperator(col_sel_src).format(ask_id = ASK_ID,
                                                  rshp_id = RSHP_ID,
                                                  prvdr_cd = prvdr_cd,
                                                  cat_cd = cat_cd)
    cur.execute(col_sel)
    return cur.fetchall()

def main():

    global WK_DTL_TP_CD,ASK_ID,RSHP_ID,BT_SEQ,BT_EXEC_SEQ,EXEC_SEQ,CNORGCODE


    logging.info('recv BT_SEQ = {}'.format(BT_SEQ))
    print('recv BT_SEQ = {}'.format(BT_SEQ))

    ClassicDeId_target_sel_01_fetchall = TBPBTV001_SEL()

    if len(ClassicDeId_target_sel_01_fetchall) == 0:
        err_msg = '상태코드 {}인 배치대상이 없습니다. == {}'.format(WK_DTL_TP_CD,
                                                                  datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        logging.info(err_msg)
        print(err_msg)
        sys.exit(1)

    BT_val = ClassicDeId_target_sel_01_fetchall[0]
    try:
        ASK_ID = BT_val[0]
        RSHP_ID = BT_val[1]
        BT_EXEC_SEQ = TBPBTV002_ins(BT_SEQ)
        # if BT_val[2] is None:
        #     err_msg = '배치등록정보에 실행일련번호(EXEC_SEQ)가 없습니다.'
        #     raise custom_checkError(err_msg)
        EXEC_SEQ = int(BT_val[2])
        CNORGCODE = BT_val[3]
        if BT_val[5] is not None:
            SRC_PTH_NM = BT_val[5] #기관 원천경로
        else :
            SRC_PTH_NM = Bobig_ori_prov_path()

        if BT_val[6] is not None:
            RSLT_PTH_NM = BT_val[6] #비식별 저장 경로
        else:
            RSLT_PTH_NM = Bobig_deid_path(CNORGCODE)

        # 원본 파일 개수
        origin_file_cnt = 0

        # 비식별 완료 파일 개수
        deid_file_cnt = 0

        msg = '=== ASK_ID : {ask_id}, RSHP_ID : {rshp_id}, SRC_PTH_NM : {src}, RSLT_PTH_NM : {rslt}'.format(ask_id = ASK_ID,
                                                                                                            rshp_id = RSHP_ID,
                                                                                                            src = SRC_PTH_NM,
                                                                                                            rslt =RSLT_PTH_NM)
        print(msg)
        logging.info(msg)

        #데이터셋 확인
        TBPINV115_sel_01_fetchall = TBPINV115_sel(ASK_ID,RSHP_ID,BT_SEQ,WK_DTL_TP_CD)


        # 데이터셋 조회 대상이 없을 경우 TBPBTV003 및 로그 기록

        if len(TBPINV115_sel_01_fetchall) == 0:
            err_msg = 'TBPINV115 신청 데이터셋 정보가 없습니다.\n' \
                      '=>ASK_ID={} RSHP_ID={} BT_SEQ = {}'.format(ASK_ID, RSHP_ID,BT_SEQ)
            raise custom_checkError(err_msg)

        #데이터셋이 존재할 경우
        for CAT_val_cnt, CAT_val in enumerate(TBPINV115_sel_01_fetchall):

            prvdr_cd = CAT_val[0]
            cat_cd = CAT_val[1]



            #TBPINB103 기관데이터 파일내역 확인
            TBPINB103_SEL_01_fetchall = TBPINB103_sel(prvdr_cd,cat_cd)

            if len(TBPINB103_SEL_01_fetchall) ==0 :
                msg = 'TBPINB103_SEL_01 select 대상이 없습니다.ASK_ID={} RSHP_ID={} PRVDR_CD={} CAT_CD={} BT_SEQ={}'\
                    .format(ASK_ID, RSHP_ID, prvdr_cd, cat_cd,BT_SEQ)
                logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                print(msg)
                TBPBTV003_ins('902', msg)
                # 대상이 없으면 다음 데이터셋을 처리
                continue
            try:
                for File_val in TBPINB103_SEL_01_fetchall:
                    FILE_NM = File_val[0]

                    # if File_val[1] != 'S':
                    #     err_msg = 'TBPINB103의 {} 상태코드(PRCS_FAIL_YN) 에러'.format(FILE_NM)
                    #     raise custom_checkError(err_msg)

                    FILE_SPLIT_NM = FILE_NM.split('_')

                    # 티베로에 등록된 원본파일 개수 증가
                    origin_file_cnt += 1

                    #연계데이터 파일이 없으면 902 기록 후 다음 파일로 continue
                    if not os.path.isfile(SRC_PTH_NM + '/' + FILE_NM):
                        err_msg = '연계데이터 파일이 없습니다. {}'.format(SRC_PTH_NM + '/' + FILE_NM)
                        raise file_Error(err_msg)

                    else :
                        # 인코딩 파악
                        #encoding_type = chardet.detect(open(SRC_PTH_NM + '/' + FILE_NM, 'rb').read())['encoding']
                        print('{} 파일 존재'.format(FILE_NM))


                        ###대체키###
                        logging.info('대체키 select start == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        #대체키 조회
                        ClassicDeId_hash_sel_01_src = SQL_DIR + '/' + 'TBPMKV_sel_01.sql'
                        ClassicDeId_hash_sel_01 = query_seperator(ClassicDeId_hash_sel_01_src).format(ask_id_date = int(ASK_ID[0:4]),
                                                                                                      ask_id = ASK_ID,
                                                                                                      rshp_id = RSHP_ID)

                        cur.execute(ClassicDeId_hash_sel_01)
                        ClassicDeId_hash_sel_01_fetchall = cur.fetchall()
                        logging.info('대체키 select end == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                        #대체키 처리
                        logging.info('대체키 처리 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        ClassicDeId_hash_data = pd.DataFrame(data = ClassicDeId_hash_sel_01_fetchall,columns = ['HASH_DID', 'ALTER_ID'])


                        ###원본 데이터###
                        # 맨 앞 컬럼 체크 ( 길이 5 이상은 상단이 컬럼. 아니면 4번째 줄이 컬럼
                        ori_col = col_check(SRC_PTH_NM,FILE_NM)

                        #메타컬럼 리스트 (컬럼이 1행에 있는지 확인하기 위함)
                        meta_col = ['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'HASH_DID', 'CAT_CD']
                        #컬럼체크 bool값
                        col_check_result = True
                        for meta in meta_col:
                            if meta not in ori_col:
                                col_check_result = False
                                break

                        #skip_row_num = 데이터 생략할 행 수
                        if col_check_result:
                            msg = '컬럼 = 1행 존재'
                            print(msg)
                            logging.info(msg)
                            skip_row_num = 1
                        elif len(ori_col) >5 :
                            msg = '컬럼없음 / 데이터 = 1행 존재 '
                            print(msg)
                            logging.info(msg)
                            get_col = get_columns(prvdr_cd,cat_cd)
                            ori_col = meta_col
                            for gcol in get_col:
                                ori_col.append(gcol[0].upper())

                            #원본데이터 컬럼 추가하기
                            ori_col_string = ','.join(ori_col)
                            with open(SRC_PTH_NM+'/'+FILE_NM,'r+',encoding='utf-8') as f:
                                s = f.read()
                                f.seek(0)
                                f.write(ori_col_string+'\n'+s)
                            skip_row_num = 1
                        else:
                            msg='컬럼 = 4행존재 / 데이터 = 9행 존재'
                            print(msg)
                            logging.info(msg)
                            temp_col = pd.read_csv(SRC_PTH_NM+'/'+FILE_NM,encoding='utf-8',skiprows=3,nrows=0).columns.tolist()
                            #대문자로 변경
                            ori_col = [t.upper() for t in temp_col]
                            skip_row_num = 8

                        # 기관데이터 불러오기
                        chunk = pd.read_csv(SRC_PTH_NM + '/' + FILE_NM,
                                                           encoding='utf-8',
                                                           header=None,
                                                           skiprows=skip_row_num,
                                                           chunksize=10000,
                                                           iterator=True,
                                                           dtype = 'string'
                                                          )

                        ClassicDeId_ori_data = pd.concat([ch for ch in chunk])

                        ClassicDeId_ori_data.columns = ori_col
                        ClassicDeId_merge_data = pd.merge(ClassicDeId_ori_data,ClassicDeId_hash_data,
                                                          how = 'left',
                                                          left_on='HASH_DID',
                                                          right_on='HASH_DID')

                        del ClassicDeId_merge_data['HASH_DID']

                        #대체키 null이 아닌 수가 0이면 902기록 후 다음 파일 수행
                        if ClassicDeId_merge_data['ALTER_ID'].notnull().sum() == 0:
                            err_msg = '{} => TBPMAT테이블과 기관데이터 매치 수 =0. 해시키 다름'.format(FILE_NM)
                            logging.info("{} == {}".format(err_msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                            TBPBTV003_ins('902', err_msg)
                            print(msg)
                            continue

                        # 대체키 실패 개수
                        alter_fail_cnt = ClassicDeId_merge_data['ALTER_ID'].isnull().sum()
                        msg = '{} 파일 대체키 처리 완료! 실패/성공 = {} / {} =='.format(FILE_NM,alter_fail_cnt,len(ClassicDeId_merge_data))\
                              + datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        print(msg)
                        logging.info(msg)
                        if alter_fail_cnt !=0:
                            TBPBTV003_ins('003',msg)
                        else:
                            TBPBTV003_ins('002', msg)

                        # 비식별화 처리 대상 조회 (TBPINV112)
                        logging.info('비식별 처리 start time == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                        ClassicDeId_target_sel_02_src = SQL_DIR + '/' + 'TBPINV112_sel_01.sql'
                        ClassicDeId_target_sel_02= query_seperator(ClassicDeId_target_sel_02_src).format(
                                                                                                         ask_id = ASK_ID,
                                                                                                         rshp_id = RSHP_ID,
                                                                                                         prvdr_cd = prvdr_cd,
                                                                                                         cat_cd = cat_cd,
                                                                                                         exec_seq = EXEC_SEQ)
                        cur.execute(ClassicDeId_target_sel_02)
                        ClassicDeId_target_sel_01_fetchall = cur.fetchall()


                        #비식별 변수가 하나도 없을경우 902 기록후 다음파일 수행
                        if ClassicDeId_target_sel_01_fetchall == 0:
                            err_msg = '파일 : {}에 대한 TBPINV112 내역이 없습니다.'.format(FILE_NM)
                            logging.info("{} == {}".format(err_msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                            TBPBTV003_ins('902', err_msg)
                            print(msg)
                            continue

                        ClassicDeId_data = ClassicDeId_merge_data

                        # # RND 문자로 바꾸기
                        # ClassicDeId_data['RND'] = ClassicDeId_data['RND'].astype(str)
                        c_column = ClassicDeId_data.columns.tolist()

                        print("파일:",FILE_NM,"\n컬럼:",c_column)
                        logging.info('TBPINV112 match cnt = {}'.format(len(ClassicDeId_target_sel_01_fetchall)))
                        print('TBPINV112 match cnt = {}'.format(len(ClassicDeId_target_sel_01_fetchall)))

                        logging.info('비식별 작업 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


                        for deid_val in ClassicDeId_target_sel_01_fetchall:
                            col_nm = deid_val[0] #비식별 컬럼이름
                            col_null_cnt = 0
                            col_fail_cnt = 0
                            col_suc_cnt = 0

                            #비식별
                            if (ClassicDeId_data[col_nm].isnull()).sum() == len(ClassicDeId_data):
                                err_msg = '비식별할 변수 {}의 파일 데이터가 Null입니다.'.format(col_nm)
                                print(err_msg)
                                logging.info(err_msg)
                                TBPBTV003_ins('003',err_msg)
                                continue

                            if col_nm in c_column:
                                if deid_val[1] is None:
                                    continue
                                else:
                                    deid_val_split = deid_val[1].split('->')

                                    #변환할 타입
                                    change_type = deid_val_split[0]
                                    #파이썬 문법
                                    di_set_val = deid_val_split[1]

                                    #null값 개수 반환
                                    col_null_cnt += (ClassicDeId_data[col_nm].isnull()).sum()

                                    #날짜 타입인 경우
                                    if change_type == 'date':
                                        temp = pd.DataFrame(ClassicDeId_data[ClassicDeId_data[col_nm].notnull()][col_nm])
                                        f1 = pd.to_datetime(temp[col_nm], format='%Y%m%d', errors='coerce')
                                        temp[col_nm] = f1.fillna('0')
                                        temp.drop(temp.loc[temp[col_nm] == '0'].index, inplace=True)

                                    # 변환할 타입이 string이 아닌경우 (int64 or float 등)
                                    elif change_type != 'string':
                                        f1 = pd.to_numeric(ClassicDeId_data[col_nm],errors='coerce').astype(change_type)
                                        temp = pd.DataFrame(f1.fillna(-999))
                                        #col_nm값이 -999인 것들 ( null / 다른 타입 등)
                                        temp.drop(temp.loc[temp[col_nm]== -999].index,inplace=True)
                                    # null이 아닌 값들로만 이루어진 컬럼데이터프레임 임시 생성

                                    else:
                                        temp = pd.DataFrame(ClassicDeId_data[ClassicDeId_data[col_nm].notnull()][col_nm])

                                    #파이썬 문법 변수 -> 데이터프레임 형식으로 변환
                                    di_set_val = str(di_set_val.replace(col_nm,
                                                                        "temp['" + col_nm + "']"))
                                    logging.info(di_set_val)
                                    temp[col_nm] = eval(di_set_val)

                                    #temp 개수 반환
                                    col_suc_cnt += len(ClassicDeId_data)-col_null_cnt

                                    #비식별 값 적용
                                    ClassicDeId_data.update(temp)
                            else:
                                err_msg = 'TBPINV112의 등록된 컬럼 {}가(이) 데이터파일 {}에 존재하지 않습니다.'.format(col_nm,
                                                                                                              FILE_NM)
                                raise file_Error(err_msg)

                            if col_fail_cnt != 0 or col_null_cnt != 0:
                                msg = '{} 의 컬럼 {} => Null값 : {} / 비식별 실패 : {} / 성공 : {}'.format(FILE_NM,
                                                                                   col_nm,
                                                                                   col_null_cnt,
                                                                                   col_fail_cnt,
                                                                                   col_suc_cnt
                                                                                   )
                                print(msg)
                                logging.info(msg)
                                TBPBTV003_ins('003',msg)

                            else :
                                msg = '컬럼 {} 비식별 완료'.format(col_nm)
                                print(msg)
                                logging.info(msg)

                        col = ClassicDeId_data.columns.tolist()
                        col = col[:3] + col[-1:] + col[3:-1]
                        ClassicDeId_data = ClassicDeId_data[col]

                        logging.info('비식별 작업 종료  == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                        # 건강보험공단(k0001)경우 801 / 아닌경우 802
                        if CNORGCODE == 'K0001':
                            FILE_SPLIT_NM[2] = '801'
                        else :
                            FILE_SPLIT_NM[2] = '802'

                        #맨 마지막 날짜가 길경우 줄여서 쓰기
                        if len(FILE_SPLIT_NM[-1]) >12:
                            FILE_SPLIT_NM[-1] = FILE_SPLIT_NM[-1][:8] +'.txt'

                        #데이터셋명 재정의
                        if len(FILE_SPLIT_NM[5:-3]) >1 :
                            for i in range(len(FILE_SPLIT_NM[5:-3])-1):
                                FILE_SPLIT_NM.remove(FILE_SPLIT_NM[5])
                        FILE_SPLIT_NM[5] = cat_cd

                        TOT_FILE_NM = "_".join(FILE_SPLIT_NM)

                        ClassicDeId_data.set_index('ASK_ID').to_csv(RSLT_PTH_NM + '/' + TOT_FILE_NM,na_rep='')

                        ####비식별 파일 생성완료###

                        #실제로 만들어진 비식별 파일 개수 증가
                        deid_file_cnt += 1

                        msg = '{dir}에 비식별 파일 {file} 생성 완료'.format(dir = RSLT_PTH_NM,
                                                                        file = TOT_FILE_NM)
                        logging.info(msg)
                        print(msg)
                        TBPBTV003_ins('002', msg)
                        TBPINV114_ins(prvdr_cd,cat_cd,TOT_FILE_NM)

                        del [[chunk,ClassicDeId_ori_data]]

            except file_Error as e:
                logging.info("{} == {}".format(e.msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                TBPBTV003_ins('902', e.msg)
                TBPINV114_ins(prvdr_cd, cat_cd, err_msg)
                print(msg)


            except:
                err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
                err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
                logging.info("{} == {}".format(err_line,err_msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                TBPBTV003_ins('902', err_msg)
                print(err_line,err_msg)


        #DB에 등록된 원본파일 개수 == 만들어진 비식별 파일 개수
        if origin_file_cnt == deid_file_cnt:
            msg = 'DB에 등록된 파일 개수 = {}개 / 만들어진 비식별 파일 개수 ={}개'.format(origin_file_cnt,deid_file_cnt)
            TBPBTV003_sel(msg)

        else:
            err_msg = 'DB에 등록된 파일 개수 = {}개 / 만들어진 비식별 파일 개수 ={}개'.format(origin_file_cnt,deid_file_cnt)
            raise custom_checkError(err_msg)


    except custom_checkError as e:
        err_msg = e.msg
        TBPBTV003_ins('902',err_msg)
        TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)
        logging.info(err_msg)
        print(err_msg)


    except:
        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
        err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        TBPBTV003_ins('902',err_msg)
        TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)
        print(err_line,err_msg)
        logging.info(err_line, err_msg)



base_file_nm = os.path.basename(__file__).split('.')
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0]+ '_' + datetime.now().strftime('%Y%m%d')   + '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')
logging.info('ClassicDeId program start time == ' +datetime.now().strftime('%Y-%m-%d %H:%M:%S'))




conn = tibero_db_conn()
cur = conn.cursor()
WK_DTL_TP_CD = '7A'
ASK_ID = ''
RSHP_ID = ''
EXEC_SEQ = 0
BT_SEQ = 0
BT_EXEC_SEQ = 0
CNORGCODE = ''

try:
    if __name__ == "__main__" :
        BT_SEQ = sys.argv[1]
        main()

except:
    err_msg = ' 에러== ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
    print(err_msg)
    logging.info(err_msg)
