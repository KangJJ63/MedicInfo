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
import inspect


class custom_checkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

def retrieve_name(var):
	#callers_local_vars = inspect.currentframe().f_back.f_locals.items()
	#return [var_name for var_name,var_val in callers_local_vars if var_val is var]
    for fi in reversed(inspect.stack()):
        names = [var_name for var_name,var_val in fi.frame.f_locals.items() if var_val is var] 
        if len(names) > 0:
            return names[0]

#def m_retrieve_name(var):
#	callers_local_vars = inspect.currentframe().f_back.f_back.f_locals.items()
#	return [var_name for var_name,var_val in callers_local_vars if var_val is var]    

def custom_logging(msg, printout=True, dbwrite=False, msg_code=""):
    msgVar = retrieve_name(msg)

    logging.info("{} : {} = {}".format(msgVar, msg, datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    if printout:
       print("{} : {}".format(msgVar, msg))

    if msg_code != "" and dbwrite:
       TBPBTV003_ins(msg_code, msg)


# 1. 배치 상태 코드가 7B 배치 조회
def TBPBTV001_SEL():
    global WK_DTL_TP_CD,BT_SEQ
    TBPBTV001_SEL_01_src = SQL_DIR + '/' + 'TBPBTV001_SEL_01.sql'
    TBPBTV001_SEL_01 = query_seperator(TBPBTV001_SEL_01_src).format(wk_dtl_tp_cd=WK_DTL_TP_CD,
                                                                    bt_seq =BT_SEQ,
                                                                    crt_pgm_id = base_file_nm[0])
    custom_logging(TBPBTV001_SEL_01)
    cur.execute(TBPBTV001_SEL_01)
    TBPBTV001_SEL_01_fetchall = cur.fetchall()
    custom_logging(len(TBPBTV001_SEL_01_fetchall))
    return TBPBTV001_SEL_01_fetchall


# 2. TBPBTV002 결과내역 시작 입력 및 BT_EXEC_SEQ 반환 함수
def TBPBTV002_ins(BT_SEQ):
    # TBPBTV002 배치결과내역에 작업 시작일시 등록
    TBPBTV002_ins_01_src = SQL_DIR + '/' + 'TBPBTV002_ins_01.sql'
    TBPBTV002_ins_01 = query_seperator(TBPBTV002_ins_01_src).format(bt_seq=BT_SEQ,
                                                                    crt_pgm_id=base_file_nm[0])
    custom_logging(TBPBTV002_ins_01)                                                                
    cur.execute(TBPBTV002_ins_01)
    conn.commit()

    # BT_EXEC_SEQ 확인
    TBPBTV002_sel_01_src = SQL_DIR + '/' + 'TBPBTV002_sel_01.sql'
    TBPBTV002_sel_01 = query_seperator(TBPBTV002_sel_01_src).format(bt_seq=BT_SEQ)
    custom_logging(TBPBTV002_sel_01)         
    cur.execute(TBPBTV002_sel_01)
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
    custom_logging(TBPINV115_sel_01)
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
    custom_logging(TBPBTV003_ins_01)            
    cur.execute(TBPBTV003_ins_01)
    conn.commit()


def TBPINB103_sel(prvdr_cd,cat_cd) :
    global ASK_ID,RSHP_ID,BT_SEQ,WK_DTL_TP_CD
    TBPINB103_SEL_01_src = SQL_DIR + '/' + 'TBPINB103_SEL_02.sql'
    TBPINB103_SEL_01 = query_seperator(TBPINB103_SEL_01_src).format(ask_id=ASK_ID,
                                                                    rshp_id=RSHP_ID,
                                                                    prvdr_cd = prvdr_cd,
                                                                    cat_cd = cat_cd)
    custom_logging(TBPINB103_SEL_01)                                                                       
    cur.execute(TBPINB103_SEL_01)
    TBPINB103_SEL_01_fetchall = cur.fetchall()
    custom_logging(len(TBPINB103_SEL_01))                                                                       
    return TBPINB103_SEL_01_fetchall



def TBPBTV003_sel():
    global BT_SEQ,BT_EXEC_SEQ
    TBPBTV003_sel_01_src = SQL_DIR + '/' + 'TBPBTV003_sel_01.sql'
    TBPBTV003_sel_01 = query_seperator(TBPBTV003_sel_01_src).format(bt_seq=BT_SEQ,
                                                                    bt_exec_seq=BT_EXEC_SEQ)
    custom_logging(TBPBTV003_sel_01)                                                                       
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
    custom_logging(TBPBTV002_upt_01)                                                                       
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
    custom_logging(TBPINV114_INS_01)                                                                       
    cur.execute(TBPINV114_INS_01)

def encode(x, y):
    if len(x) == 8 and y is not None and math.isnan(y) == False:
        return ((datetime.strptime(x, '%Y%m%d')) + timedelta(days=float(y))).strftime('%Y%m%d')
    elif x=='0':
        return '00000000'
    else :
        return ''

def col_check(path,file):
    columns = list()
    try:
        with open(path+'/'+file, encoding='utf-8') as f:
            limit = 1
            count = 0

            for line in f:
                columns = [word.upper() for word in line[:-1].split(',')]

                count += 1
                if count >= limit:
                    break
    except:
        with open(path+'/'+file, encoding='euc-kr') as f:
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

def file_write(path,file,df,cnt):
    if cnt == 0 :
        write_type = 'w'
    else:
        write_type = 'a'
    with open(path+'/'+file,mode=write_type,encoding='utf-8') as f:
        if write_type == 'w':
            a = ','.join(df.columns)
            f.writelines(a + '\n')
        for idx in df.index:
            row_text = ','.join(df.iloc[idx].astype(str))
            f.writelines(row_text+'\n')

def main():

    global WK_DTL_TP_CD,ASK_ID,RSHP_ID,BT_SEQ,BT_EXEC_SEQ,EXEC_SEQ,CNORGCODE
    custom_logging(BT_SEQ)

    ClassicDeId_target_sel_01_fetchall = TBPBTV001_SEL()
    if len(ClassicDeId_target_sel_01_fetchall) == 0:
        batch_msg = '배치작업상세코드(WK_DTL_TP_CD) {}인 배치대상이 없습니다.'.format(WK_DTL_TP_CD)
        custom_logging(batch_msg)
        sys.exit(1)

    BT_val = ClassicDeId_target_sel_01_fetchall[0]
    try:
        ASK_ID = BT_val[0]
        RSHP_ID = BT_val[1]
        BT_EXEC_SEQ = TBPBTV002_ins(BT_SEQ)
        if BT_val[2] is None:
            err_msg = '배치등록정보에 실행일련번호(EXEC_SEQ)가 없습니다.'
            raise custom_checkError(err_msg)

        if EXEC_SEQ == "":
            EXEC_SEQ = int(BT_val[2])
        CNORGCODE = BT_val[3]
        SRC_PTH_NM = BT_val[5] #기관 원천경로
        RSLT_PTH_NM = BT_val[6] #비식별 저장 경로

        msg = '=== ASK_ID : {ask_id}, RSHP_ID : {rshp_id}, SRC_PTH_NM : {src}, RSLT_PTH_NM : {rslt}'.format(ask_id = ASK_ID,
                                                                                                            rshp_id = RSHP_ID,
                                                                                                            src = SRC_PTH_NM,
                                                                                                            rslt =RSLT_PTH_NM)
        custom_logging(msg)

        #데이터셋 확인
        TBPINV115_sel_01_fetchall = TBPINV115_sel(ASK_ID,RSHP_ID,BT_SEQ,WK_DTL_TP_CD)

        # 데이터셋 조회 대상이 없을 경우 TBPBTV003 및 로그 기록
        if len(TBPINV115_sel_01_fetchall) == 0:
            err_msg = 'TBPINV115 신청 데이터셋 정보가 없습니다.\n' \
                      '=>ASK_ID={} RSHP_ID={} BT_SEQ = {}'.format(ASK_ID, RSHP_ID,BT_SEQ)
            raise custom_checkError(err_msg)

        #대체키 조회 : 대체키 조회는 ASK_ID,RSHP_ID 이 바뀔때만 필요
        ClassicDeId_hash_sel_01_src = SQL_DIR + '/' + 'TBPMAT_sel_01.sql'
        ClassicDeId_hash_sel_01 = query_seperator(ClassicDeId_hash_sel_01_src).format(ask_id_date = int(ASK_ID[0:4]),
                                                                                        ask_id = ASK_ID,
                                                                                        rshp_id = RSHP_ID)

        custom_logging(ClassicDeId_hash_sel_01)
        cur.execute(ClassicDeId_hash_sel_01)
        ClassicDeId_hash_sel_01_fetchall = cur.fetchall()
        custom_logging(len(ClassicDeId_hash_sel_01_fetchall))

        #데이터셋이 존재할 경우
        for CAT_val_cnt, CAT_val in enumerate(TBPINV115_sel_01_fetchall):

            prvdr_cd = CAT_val[2]
            cat_cd = CAT_val[3]
            custom_logging(prvdr_cd)
            custom_logging(cat_cd)

            #TBPINB103 기관데이터 파일내역 확인
            TBPINB103_SEL_01_fetchall = TBPINB103_sel(prvdr_cd, cat_cd)

            if len(TBPINB103_SEL_01_fetchall) == 0 :
                msg = 'TBPINB103_SEL_01 select 대상이 없습니다.ASK_ID={} RSHP_ID={} PRVDR_CD={} CAT_CD={} BT_SEQ={}'\
                    .format(ASK_ID, RSHP_ID, prvdr_cd, cat_cd,BT_SEQ)
                custom_logging(msg, STDOUT_TRUE, TBPBTV003_TRUE, '902')    
                # 대상이 없으면 다음 데이터셋을 처리
                continue

            for File_val in TBPINB103_SEL_01_fetchall:
                FILE_NM = File_val[0]
                FILE_SPLIT_NM = FILE_NM.split('_')
                custom_logging(FILE_NM) 
                custom_logging(FILE_SPLIT_NM) 
                
                if not os.path.isfile(SRC_PTH_NM + '/' + FILE_NM):
                    msg = '연계데이터 파일이 없습니다. {}'.format(SRC_PTH_NM + '/' + FILE_NM)
                    custom_logging(msg, STDOUT_TRUE, TBPBTV003_TRUE, '902')  
                    continue

                else:
                    custom_logging('{} 파일 존재'.format(FILE_NM)) 
                    #대체키 처리
                    ClassicDeId_hash_data = pd.DataFrame(data = ClassicDeId_hash_sel_01_fetchall, columns = ['HASH_DID', 'ALTER_ID', 'RND'])
                    custom_logging(ClassicDeId_hash_data) 
                    
                    ###원본 데이터###
                    # 맨 앞 컬럼 체크 ( 길이 5 이상은 상단이 컬럼. 아니면 4번째 줄이 컬럼
                    ori_col = col_check(SRC_PTH_NM, FILE_NM)

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
                        skip_row_num = 1
                        column_msg = '컬럼 = 1행 존재 skip_row = {}'.format(skip_row_num)
                    elif len(ori_col) > 5 :
                        get_col = get_columns(prvdr_cd,cat_cd)
                        ori_col = meta_col
                        for gc in get_col:
                            ori_col.append(gc[0].upper())
                        column_msg = '컬럼없음 / 데이터 = 1행 존재 skip_row = {}'.format(skip_row_num)
                        skip_row_num = 0
                    else:
                        try:
                            temp_col = pd.read_csv(SRC_PTH_NM+'/'+FILE_NM,encoding='utf-8',skiprows=3,nrows=0).columns.tolist()
                        except:
                            temp_col = pd.read_csv(SRC_PTH_NM+'/'+FILE_NM,encoding='euc-kr',skiprows=3,nrows=0).columns.tolist()
                        #대문자로 변경
                        ori_col = [t.upper() for t in temp_col]
                        skip_row_num = 8
                        msg = '컬럼 = 4행존재 / 데이터 = 9행 존재 skip_row = {}'.format(skip_row_num)

                    custom_logging(msg) 
                    custom_logging(ori_col) 

                    # 기관데이터 불러오기
                    try:
                        #기관데이터 불러오기
                        ClassicDeId_ori_data = pd.read_csv(SRC_PTH_NM + '/' + FILE_NM,
                                                        names=ori_col,
                                                        header=None,
                                                        encoding='utf-8',
                                                        dtype = str,
                                                        na_filter=None,
                                                        skiprows=skip_row_num)
                    except:
                        #기관데이터 불러오기
                        ClassicDeId_ori_data = pd.read_csv(SRC_PTH_NM + '/' + FILE_NM,
                                                        names=ori_col,
                                                        header=None,
                                                        encoding='euc-kr',
                                                        dtype = str,
                                                        na_filter=None,
                                                        skiprows=skip_row_num)
                    custom_logging(len(ClassicDeId_ori_data))
                    custom_logging(ClassicDeId_ori_data)

                    # 대체키 null 이면 결합하지 않는다
                    if PASS_ALTER_ID:
                        ClassicDeId_merge_data = pd.merge(ClassicDeId_ori_data,ClassicDeId_hash_data,
                                                        how = 'inner',
                                                        left_on='HASH_DID',
                                                        right_on='HASH_DID')
                    else:
                        ClassicDeId_merge_data = pd.merge(ClassicDeId_ori_data,ClassicDeId_hash_data,
                                                        how = 'left',
                                                        left_on='HASH_DID',
                                                        right_on='HASH_DID')

                    # 대체키 null이 아닌 수가 0이면 에러 후 실패
                    if ClassicDeId_merge_data['ALTER_ID'].notnull().sum() == 0:
                        err_msg = '{} {} {} {} {} TBPMAT테이블과 기관데이터 매치 수 = 0. 매치되는 해시키 없음'.format(
                            len(ClassicDeId_merge_data), len(ClassicDeId_hash_data), ASK_ID,RSHP_ID,FILE_NM)
                        raise custom_checkError(err_msg)

                    #대체키 성공 개수
                    alter_suc_cnt = ClassicDeId_merge_data['ALTER_ID'].notnull().sum()
                    # 대체키 실패 개수
                    alter_fail_cnt = ClassicDeId_merge_data['ALTER_ID'].isnull().sum()
                    msg = '{} 파일 대체키 처리 완료! 실패건수/총건수 = {} / {}'.format(FILE_NM,alter_fail_cnt,len(ClassicDeId_merge_data))
                    custom_logging(msg)
                    
                    if alter_fail_cnt > 0:
                        custom_logging(msg, STDOUT_TRUE, TBPBTV003_TRUE, '003')
                    else:
                        custom_logging(msg, STDOUT_TRUE, TBPBTV003_TRUE, '002')
                    del ClassicDeId_merge_data['HASH_DID']

                    # 비식별화 처리 대상 조회 (TBPINV112)
                    # logging.info('비식별 처리 start time == ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    ClassicDeId_target_sel_02_src = SQL_DIR + '/' + 'TBPINV112_sel_01.sql'
                    ClassicDeId_target_sel_02 = query_seperator(ClassicDeId_target_sel_02_src).format(ask_id = ASK_ID,
                                                                                rshp_id = RSHP_ID,
                                                                                prvdr_cd = prvdr_cd,
                                                                                cat_cd = cat_cd,
                                                                                exec_seq = EXEC_SEQ)
                    custom_logging(ClassicDeId_target_sel_02)                                                            
                    cur.execute(ClassicDeId_target_sel_02)
                    ClassicDeId_target_sel_01_fetchall = cur.fetchall()

                    #컬럼 비식별 실패 개수 데이터프레임
                    # col_fail_df = pd.DataFrame(columns=['COL','FAIL_CNT'])
                    #저장할 파일 이름 정의
                    if CNORGCODE == 'K0001':
                        FILE_SPLIT_NM[2] = '801'
                    else :
                        FILE_SPLIT_NM[2] = '802'

                    #맨 마지막 날짜가 길경우 줄여서 쓰기
                    if len(FILE_SPLIT_NM[-1]) >12:
                        FILE_SPLIT_NM[-1] = FILE_SPLIT_NM[-1][:8] +'.txt'

                    #데이터셋명 재정의
                    if ASK_ID == "2018-00003":
                        FILE_SPLIT_NM[3] = ASK_ID[:4] + ASK_ID[5:]
                    if len(FILE_SPLIT_NM[5:-3]) >1 :
                        FILE_SPLIT_NM.remove(FILE_SPLIT_NM[5])
                    FILE_SPLIT_NM[5] = cat_cd
                    TOT_FILE_NM = "_".join(FILE_SPLIT_NM)

                    if len(ClassicDeId_target_sel_01_fetchall) == 0:
                        err_msg = '파일 : {}에 대한 TBPINV112 내역이 없습니다.'.format(FILE_NM)
                        custom_logging(err_msg, STDOUT_TRUE, TBPBTV003_TRUE, '003')
                        if PASS_NOT_EXISTS_TBPINV112:
                            pass_msg = '비식별파일{} 을 생성하지 않음.'.format(TOT_FILE_NM) 
                            custom_logging(pass_msg)
                            continue

                    else:
                        # 비식별 조회건이 0이 아니면 비식별처리
                        c_column = ClassicDeId_merge_data.columns.tolist()
                        for deid_val in ClassicDeId_target_sel_01_fetchall:
                            #try / except로 여기서 에러날경우 에러 표시후 다음파일로 진행
                            try:
                                col_fail_cnt = 0
                                col_convert_fail_cnt = 0
                                col_null_cnt = 0
                                col_nm = deid_val[0]  # 컬럼명

                                if col_nm in c_column:
                                    if deid_val[1] is None:
                                        continue
                                    else:
                                        deid_val_split = deid_val[1].split('->')
                                        #변환할 타입
                                        change_type = deid_val_split[0]
                                        di_set_val = deid_val_split[1] # 비식별 파이썬 문법
                                        
                                        #날짜 타입인 경우
                                        if change_type == 'date':
                                            f1 = pd.to_datetime(ClassicDeId_merge_data[col_nm], format='%Y%m%d', errors='coerce').dt.strftime('%Y%m%d')
                                            temp = pd.DataFrame(f1.fillna('0'))
                                            #ClassicDeId_merge_data[col_nm] = np.where(ClassicDeId_merge_data[col_nm] == '',  ClassicDeId_merge_data[col_nm], f1.fillna('0'))

                                            col_convert_fail_cnt = (temp[col_nm] == '0').sum()
                                            temp.drop(temp.loc[temp[col_nm] == '0'].index, inplace=True)
                                            temp = pd.merge(temp, ClassicDeId_merge_data['RND'], left_index = True, right_index = True, how = 'inner')
                                            custom_logging(temp)

                                        # 변환할 타입이 string이 아닌경우 (int64 or float 등)
                                        elif change_type != 'string':
                                            f1 = pd.to_numeric(ClassicDeId_merge_data[col_nm],errors='coerce').astype(change_type)
                                            temp = pd.DataFrame(f1.fillna(-999))
                                            col_convert_fail_cnt = (temp[col_nm] == -999).sum()
                                            #col_nm값이 -999인 것들 ( null / 다른 타입 등)
                                            temp.drop(temp.loc[temp[col_nm] == -999].index,inplace=True)

                                        # null이 아닌 값들로만 이루어진 컬럼데이터프레임 임시 생성
                                        else:
                                            temp = pd.DataFrame(ClassicDeId_merge_data[ClassicDeId_merge_data[col_nm].notnull()][col_nm])

                                        custom_logging("비식별 전: {}".format(ClassicDeId_merge_data[col_nm]))
                                        col_null_cnt = (temp[col_nm]=='').sum()
                                        if col_convert_fail_cnt + col_null_cnt > 0:
                                            deid_result_msg = '컬럼 {} CONVET 오류 = {} NULL = {}'.format(col_nm, col_convert_fail_cnt, col_null_cnt)
                                            custom_logging(deid_result_msg, STDOUT_TRUE, TBPBTV003_TRUE, '003')

                                        #파이썬 문법 변수 -> 데이터프레임 형식으로 변환
                                        if ASK_ID == "2018-00003" and change_type == 'date':
                                            print("비식별기준 : 일자 + random(1,15)") 
                                            temp[col_nm] = temp.apply(lambda x: encode(x[col_nm], x['RND']), axis=1)
                                            di_msg = "컬럼명: {}, 비식별기준: {}".format(col_nm, di_set_val)
                                            custom_logging(di_msg)
                                        else:
                                            eval_di_set_val = str(di_set_val.replace(col_nm, "temp['" + col_nm + "']"))
                                            custom_logging(eval_di_set_val)
                                            temp[col_nm] = eval(eval_di_set_val)
                                            di_msg = "컬럼명: {}, 비식별기준: {}, 적용: {}".format(col_nm, di_set_val, eval_di_set_val)
                                            custom_logging(di_msg)
                                        
                                        #비식별 값 적용
                                        ClassicDeId_merge_data.update(temp[col_nm])
                                        custom_logging("비식별 후: {}".format(ClassicDeId_merge_data[col_nm]))

                                else:
                                    err_msg = 'TBPINV112의 등록된 컬럼 {}가(이) 데이터파일 {}에 존재하지 않습니다.'.format(col_nm,
                                                                                                        FILE_NM)
                                    TBPINV114_ins(prvdr_cd, cat_cd, err_msg)
                                    custom_logging(err_msg, STDOUT_TRUE, TBPBTV003_TRUE, '902')
                                    continue

                            except:
                                #에러 만들었던 비식별 파일 삭제
                                #if os.path.isfile(RSLT_PTH_NM + '/' + TOT_FILE_NM):
                                #    os.remove(RSLT_PTH_NM + '/' + TOT_FILE_NM)
                                err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
                                err_msg = '에러 :'+''.join(map(str, sys.exc_info()[:2])).replace('\'', '')
                                custom_logging(err_line)
                                custom_logging(err_msg, STDOUT_TRUE, TBPBTV003_TRUE, '902')
                                continue
                        # end for deid_val in ClassicDeId_target_sel_01_fetchall:

                    # 비식별기준이 없어도 모든 파일을 만든다  
                    # RND 삭제  
                    del ClassicDeId_merge_data['RND']  
                    col = ClassicDeId_merge_data.columns.tolist()
                    aftercol = col[:3] + col[-1:] + col[3:-1]
                    custom_logging("컬럼순서 변경전 {}\n변경후{}".format(col, aftercol))
                    ClassicDeId_merge_data = ClassicDeId_merge_data[aftercol]
                    custom_logging(ClassicDeId_merge_data)

                    if ASK_ID == "2018-00003":
                        ClassicDeId_merge_data['ASK_ID'] = '2018-00003'

                    ClassicDeId_merge_data.set_index('ASK_ID').to_csv(RSLT_PTH_NM + '/' + TOT_FILE_NM)

                    custom_logging('데이터 건수 = {} '.format(len(ClassicDeId_merge_data)))
                    msg = '{dir}에 비식별 파일 {file} 생성 완료'.format(dir=RSLT_PTH_NM,file=TOT_FILE_NM)
                    custom_logging(msg, STDOUT_TRUE, TBPBTV003_TRUE, '002')
                    TBPINV114_ins(prvdr_cd,cat_cd,TOT_FILE_NM)

                    del [[ClassicDeId_ori_data, ClassicDeId_merge_data]]
                    #gc.collect()

            # end for File_val in TBPINB103_SEL_01_fetchall:

        TBPBTV003_sel()

    except custom_checkError as e:
        err_msg = e.msg
        custom_logging(err_msg, STDOUT_TRUE, TBPBTV003_TRUE, '902')
        TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)


    except:
        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
        err_msg = ' 에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        custom_logging(err_line)
        custom_logging(err_msg, STDOUT_TRUE, TBPBTV003_TRUE, '902')

        TBPBTV003_ins('902', err_msg)
        TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)


##################################################
#         

base_file_nm = os.path.basename(__file__).split('.')
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0]  + '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')
logging.info('{} program start time == '.format(base_file_nm[0]) +datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

conn = tibero_db_conn()
cur = conn.cursor()
WK_DTL_TP_CD = '7A'
ASK_ID = ''
RSHP_ID = ''
EXEC_SEQ = 0
BT_SEQ = 0
BT_EXEC_SEQ = 0
CNORGCODE = ''
# 비식별기준이 없으면 파일 생성을 하지 않는다. 비식별만 재작업하기 위하여
PASS_NOT_EXISTS_TBPINV112 = False  # default = True
PASS_ALTER_ID = True # default = True

STDOUT_TRUE = True # default
STDOUT_FALSE = False
TBPBTV003_TRUE = True
TBPBTV003_FALSE = False # default

try:
    if __name__ == "__main__" :
        BT_SEQ = sys.argv[1]
        EXEC_SEQ = sys.argv[2]
        main()

except:
    err_msg = ' 에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
    print(err_msg)
    logging.info(err_msg)
