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



def TBPBTV003_sel():
    global BT_SEQ,BT_EXEC_SEQ
    TBPBTV003_sel_01_src = SQL_DIR + '/' + 'TBPBTV003_sel_01.sql'
    TBPBTV003_sel_01 = query_seperator(TBPBTV003_sel_01_src).format(bt_seq=BT_SEQ,
                                                                    bt_exec_seq=BT_EXEC_SEQ)
    cur.execute(TBPBTV003_sel_01)
    TBPBTV003_sel_01_fetchall = cur.fetchall()
    for fail_val_cnt, fail_val in enumerate(TBPBTV003_sel_01_fetchall):
        FAIL_CNT = fail_val[0]
        if FAIL_CNT == 0:
            TBPBTV002_upt('002', '파이썬 작업 성공', '비식별 작업 성공')
        else:
            TBPBTV002_upt('902', '파이썬 작업 실패',  '비식별 작업 실패. 실패파일 개수 ={}개'.format(FAIL_CNT))


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

def encode(x, y):
    if len(x) == 8 and y is not None and math.isnan(y) == False:
        return ((datetime.strptime(x, '%Y%m%d')) + timedelta(days=float(y))).strftime('%Y%m%d')
    elif x=='0':
        return '00000000'
    else :
        return ''

def col_check(path,file,encode):
    columns = list()
    with open(path+'/'+file, encoding=encode) as f:
        limit = 1
        count = 0

        for line in f:
            columns = [word.upper() for word in line[:-1].split(',')]

            count += 1
            if count >= limit:
                break
    return columns

def get_columns(prvdr_cd,cat_cd):
    global ASK_ID,RSHP_ID
    col_sel_src = SQL_DIR +'/' + 'TBPINM102_col_sel_01.sql'
    col_sel = query_seperator(col_sel_src).format(ask_id = ASK_ID,
                                                  rshp_id = RSHP_ID,
                                                  prvdr_cd = prvdr_cd,
                                                  cat_cd = cat_cd)
    cur.execute(col_sel)
    return cur.fetchall()

def file_write(path,file,df,cnt,encoding_type):
    if cnt == 0 :
        write_type = 'w'
    else:
        write_type = 'a'

    if encoding_type.upper() == 'UTF-8':
        encod = 'utf-8'
    else :
        encod = 'utf-8-sig'

    with open(path+'/'+file,mode=write_type,encoding=encod) as f:
        if write_type == 'w':
            a = ','.join(df.columns)
            f.writelines(a + '\n')
        for idx in range(len(df)):
            row_text = ','.join(df.iloc[idx].astype(str))
            f.writelines(row_text+'\n')

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
        if BT_val[2] is None:
            err_msg = '배치등록정보에 실행일련번호(EXEC_SEQ)가 없습니다.'
            raise custom_checkError(err_msg)
        EXEC_SEQ = int(BT_val[2])
        CNORGCODE = BT_val[3]
        SRC_PTH_NM = BT_val[5] #기관 원천경로
        RSLT_PTH_NM = BT_val[6] #비식별 저장 경로

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

            prvdr_cd = CAT_val[2]
            cat_cd = CAT_val[3]


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

            for File_val in TBPINB103_SEL_01_fetchall:
                FILE_NM = File_val[0]

                # if File_val[1] != 'S':
                #     err_msg = 'TBPINB103의 {} 상태코드(PRCS_FAIL_YN) 에러'.format(FILE_NM)
                #     raise custom_checkError(err_msg)

                FILE_SPLIT_NM = FILE_NM.split('_')

                if not os.path.isfile(SRC_PTH_NM + '/' + FILE_NM):
                    msg = '연계데이터 파일이 없습니다. {}'.format(SRC_PTH_NM + '/' + FILE_NM)
                    logging.info("{} == {}".format(msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
                    TBPBTV003_ins('902', msg)
                    print(msg)
                    continue

                else:
                    # 인코딩 파악
                    encoding_type = chardet.detect(open(SRC_PTH_NM + '/' + FILE_NM, 'rb').read())['encoding']
                    print('{} 파일 존재 , encod type = {}'.format(FILE_NM,encoding_type))
                    #logging.info('대체키 select start == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


                    ###대체키###

                    #대체키 조회
                    ClassicDeId_hash_sel_01_src = SQL_DIR + '/' + 'TBPMAT_sel_02.sql'
                    ClassicDeId_hash_sel_01 = query_seperator(ClassicDeId_hash_sel_01_src).format(ask_id_date = int(ASK_ID[0:4]),
                                                                                                  ask_id = ASK_ID,
                                                                                                  rshp_id = RSHP_ID)

                    cur.execute(ClassicDeId_hash_sel_01)
                    ClassicDeId_hash_sel_01_fetchall = cur.fetchall()
                    logging.info('대체키 select end == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

                    #대체키 처리
                    #logging.info('대체키 처리 시작 == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    ClassicDeId_hash_data = pd.DataFrame(data = ClassicDeId_hash_sel_01_fetchall,columns = ['HASH_DID', 'ALTER_ID'])


                    ###원본 데이터###
                    # 맨 앞 컬럼 체크 ( 길이 5 이상은 상단이 컬럼. 아니면 4번째 줄이 컬럼
                    ori_col = col_check(SRC_PTH_NM,FILE_NM,encoding_type)

                    #메타컬럼 리스트 (컬럼이 1행에 있는지 확인하기 위함)
                    meta_col = ['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'HASH_DID', 'DATASET']
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
                        skip_row_num = 0
                    elif len(ori_col) > 5 :
                        msg = '컬럼없음 / 데이터 = 1행 존재 '
                        print(msg)
                        logging.info(msg)
                        get_col = get_columns(prvdr_cd,cat_cd)
                        ori_col = meta_col
                        for gc in get_col:
                            ori_col.append(gc[0].upper())

                        #원본데이터 컬럼 추가하기
                        ori_col_string = ','.join(ori_col)
                        with open(SRC_PTH_NM+'/'+FILE_NM,'r+') as f:
                            s = f.read()
                            f.seek(0)
                            f.write(ori_col_string+'\n'+s)
                        skip_row_num = 0
                    else:
                        msg='컬럼 = 4행존재 / 데이터 = 9행 존재'
                        print(msg)
                        logging.info(msg)
                        temp_col = pd.read_csv(SRC_PTH_NM+'/'+FILE_NM,encoding=encoding_type,skiprows=3,nrows=0).columns.tolist()
                        #대문자로 변경
                        ori_col = [t.upper() for t in temp_col]
                        skip_row_num = 7

                    # 기관데이터 불러오기
                    chunk = pd.read_csv(SRC_PTH_NM + '/' + FILE_NM,
                                                       encoding=encoding_type,
                                                       dtype = str,
                                                       chunksize=10000,
                                                       na_filter=None,
                                                       iterator=True,
                                                       skiprows=skip_row_num
                                                      )

                    # 비식별화 처리 대상 조회 (TBPINV112)
                    logging.info('비식별 처리 start time == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    ClassicDeId_target_sel_02_src = SQL_DIR + '/' + 'TBPINV112_sel_encode_01.sql'
                    ClassicDeId_target_sel_02= query_seperator(ClassicDeId_target_sel_02_src).format(
                                                                                                     ask_id = ASK_ID,
                                                                                                     rshp_id = RSHP_ID,
                                                                                                     prvdr_cd = prvdr_cd,
                                                                                                     cat_cd = cat_cd,
                                                                                                     exec_seq = EXEC_SEQ)
                    cur.execute(ClassicDeId_target_sel_02)
                    ClassicDeId_target_sel_01_fetchall = cur.fetchall()

                    if len(ClassicDeId_target_sel_01_fetchall) == 0:
                        err_msg = '파일 : {}에 대한 TBPINV112 내역이 없습니다.'.format(FILE_NM)
                        print(err_msg)
                        logging.info(err_msg)
                        TBPBTV003_ins('003', err_msg)

                    #대체키 성공 개수
                    alter_suc_cnt=0
                    # 대체키 실패 개수
                    alter_fail_cnt = 0
                    #컬럼 비식별 실패 개수 데이터프레임
                    col_fail_df = pd.DataFrame(columns=['COL','FAIL_CNT'])
                    #저장할 파일 이름 정의
                    if CNORGCODE == 'K0001':
                        FILE_SPLIT_NM[2] = '801'
                    else :
                        FILE_SPLIT_NM[2] = '802'

                    #맨 마지막 날짜가 길경우 줄여서 쓰기
                    if len(FILE_SPLIT_NM[-1]) >12:
                        FILE_SPLIT_NM[-1] = FILE_SPLIT_NM[-1][:8] +'.txt'

                    #데이터셋명 재정의
                    if len(FILE_SPLIT_NM[5:-3]) >1 :
                        FILE_SPLIT_NM.remove(FILE_SPLIT_NM[5])
                    FILE_SPLIT_NM[5] = cat_cd

                    TOT_FILE_NM = "_".join(FILE_SPLIT_NM)

                    #try / except로 여기서 에러날경우 에러 표시후 다음파일로 진행
                    try:
                        cnt = 0
                        for ch in chunk:
                            ch.columns = ori_col
                            ClassicDeId_merge_data = pd.merge(ch,ClassicDeId_hash_data,
                                                              how = 'left',
                                                              left_on='HASH_DID',
                                                              right_on='HASH_DID')
                            del ClassicDeId_merge_data['HASH_DID']
                            alter_suc_cnt += ClassicDeId_merge_data['ALTER_ID'].notnull().sum()
                            alter_fail_cnt += ClassicDeId_merge_data['ALTER_ID'].isnull().sum()
                            ClassicDeId_data = ClassicDeId_merge_data

                            #비식별 조회건이 0이면
                            if len(ClassicDeId_target_sel_01_fetchall) != 0:
                                c_column = ClassicDeId_data.columns.tolist()
                                for deid_val in ClassicDeId_target_sel_01_fetchall:
                                    col_nm = deid_val[0]  # 비식별 컬럼이름
                                    col_fail_cnt = 0
                                    # 해당일자의 컬럼들이면 로직 수행
                                    if col_nm in ['RECU_FR_DD', 'RECU_TO_DD', 'FST_DD']:
                                        f1 = pd.to_datetime(ClassicDeId_data[col_nm], format='%Y%m%d',
                                                            errors='coerce').dt.strftime('%Y%m%d')
                                        ClassicDeId_data[col_nm] = np.where(ClassicDeId_data[col_nm] == '',
                                                                            ClassicDeId_data[col_nm], f1.fillna('0'))
                                        ClassicDeId_data[col_nm] = ClassicDeId_data.apply(
                                            lambda x: encode(x[col_nm], 1), axis=1)
                                        col_fail_cnt += (ClassicDeId_data[col_nm] == '00000000').sum()
                                        col_fail_cnt += (ClassicDeId_data[col_nm] == '').sum()
                                        if col_fail_cnt > 0 :
                                            col_fail_df = col_fail_df.append(pd.Series([col_nm,col_fail_cnt],
                                                                                       index=col_fail_df.columns),
                                                                                       ignore_index=True)

                                    elif col_nm in c_column:
                                        if deid_val[1] is None:
                                            continue
                                        else:
                                            di_set_val = deid_val[1]  # 비식별 파이썬 문법
                                            di_set_val = str(di_set_val.replace(col_nm,
                                                                                "ClassicDeId_data['" + col_nm + "']"))
                                            logging.info(di_set_val)
                                            ClassicDeId_data[col_nm] = eval(di_set_val)

                                        # col_fail_cnt = (ClassicDeId_data[col_nm]=='').sum()
                                    else:
                                        err_msg = 'TBPINV112의 등록된 컬럼 {}가(이) 데이터파일 {}에 존재하지 않습니다.'.format(col_nm,
                                                                                                         FILE_NM)
                                        TBPINV114_ins(prvdr_cd, cat_cd, err_msg)
                                        TBPBTV003_ins('902', err_msg)
                                        continue
                            # del ClassicDeId_data['RND']
                            col = ClassicDeId_data.columns.tolist()
                            col = col[:3] + col[-1:] + col[3:-1]

                            ClassicDeId_data = ClassicDeId_data.reindex(columns = col)
                            file_write(RSLT_PTH_NM,TOT_FILE_NM,ClassicDeId_data,cnt,encoding_type)
                            cnt+=1
                    except:
                        #에러나면 만들었던 비식별 파일 삭제
                        if os.path.isfile(RSLT_PTH_NM + '/' + TOT_FILE_NM):
                            os.remove(RSLT_PTH_NM + '/' + TOT_FILE_NM)
                        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
                        err_msg = '에러 :'+''.join(map(str, sys.exc_info()[:2])).replace('\'', '')
                        print(err_line,err_msg)
                        TBPBTV003_ins('902',err_msg)
                        continue

                    #만약 대체키 성공횟수가 0이면 파일 삭제
                    if alter_suc_cnt == 0:
                        if os.path.isfile(RSLT_PTH_NM+'/'+TOT_FILE_NM):
                            os.remove(RSLT_PTH_NM+'/'+TOT_FILE_NM)
                        err_msg = '{} 대체키 매치 수 = 0개. 비식별 실패'.format(FILE_NM)
                        print(err_msg)
                        logging.info(err_msg)
                        TBPBTV003_ins('902',err_msg)
                        continue
                    else:
                        msg = '{} 파일 대체키 처리 완료! 실패/성공 = {} / {}'.format(FILE_NM,alter_fail_cnt,alter_suc_cnt)
                        print(msg)
                        logging.info(msg)
                        TBPBTV003_ins('003',msg)

                    if len(col_fail_df) > 0 :
                        for idx in col_fail_df.index:
                            c_name = col_fail_df._get_value(idx,'COL')
                            c_fail_cnt = col_fail_df._get_value(idx,'FAIL_CNT')

                            err_msg = '컬럼 {} 비식별 실패 개수 = {}개'.format(c_name,c_fail_cnt)
                            print(err_msg)
                            logging.info(err_msg)
                            TBPBTV003_ins('003',err_msg)

                    msg = '{dir}에 비식별 파일 {file} 생성 완료'.format(dir=RSLT_PTH_NM,file=TOT_FILE_NM)
                    print(msg)
                    logging.info(msg)
                    TBPBTV003_ins('002',msg)
        TBPBTV003_sel()


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
    filename=LOG_DIR + '/' + base_file_nm[0]  + '.log', \
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
