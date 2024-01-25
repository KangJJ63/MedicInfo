import os, sys
import logging
from datetime import datetime,timedelta
import inspect
import pandas as pd
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.base_bobig import *
from base.tibero_dbconn2 import *
from base.query_sep import *

def retrieve_name(var):
    #callers_local_vars = inspect.currentframe().f_back.f_locals.items()
    #return [var_name for var_name,var_val in callers_local_vars if var_val is var]
    for fi in reversed(inspect.stack()):
        names = [var_name for var_name,var_val in fi.frame.f_locals.items() if var_val is var] 
        if len(names) > 0:
            return names[0]

# def m_retrieve_name(var):
# callers_local_vars = inspect.currentframe().f_back.f_back.f_locals.items()
# return [var_name for var_name,var_val in callers_local_vars if var_val is var]    

def custom_logging(msg, printout=True, dbwrite=False, msg_code=""):
    msgVar = retrieve_name(msg)
    logging.info("{} : {}".format(msgVar, msg))
    if printout:
       print("{} : {}".format(msgVar, msg))
    if msg_code != "" and dbwrite:
       TBPBTV003_ins(msg_code, msg)

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

#################################################################
def main():
    # File Read
    TBIPKV_data = pd.read_csv(SRC_PTH_NM + '/' + FILE_NM,
                            names=['ASK_ID','RSHP_ID','PRVDR_CD','HASH_DID'],
                            index_col=False,
                            header=None,
                            encoding='utf-8',
                            dtype = str,
                            na_filter=None,
                            skiprows=1)
    custom_logging(TBIPKV_data.shape) 
    custom_logging(TBIPKV_data) 
    # 컬럼순서 재정의
    TBIPKV_data = TBIPKV_data[['HASH_DID']]
    custom_logging(TBIPKV_data) 
    # sql에 한번에 넣기 위해 투플로 저장
    TBIPKV_data = TBIPKV_data.fillna('').values.tolist()

    if TRUNC_FLAG == 'Y':
        TBIPKV_DEL_01_src = SQL_DIR + '/' + 'TBIPKV_DEL_01.sql'
        TBIPKV_DEL_01 = query_seperator(TBIPKV_DEL_01_src).format(year=ASK_ID[0:4],
                                                                ask_id_num=ASK_ID[5:],
                                                                rshp_id=RSHP_ID,
                                                                prvdr_cd=PRVDR_CD)
        custom_logging(TBIPKV_DEL_01)            
        cur2.execute(TBIPKV_DEL_01)

    TBIPKV_INS_01_src = SQL_DIR + '/' + 'TBIPKV_INS_01.sql'
    TBIPKV_INS_01 = query_seperator(TBIPKV_INS_01_src).format(year=ASK_ID[0:4],
                                                            ask_id=ASK_ID,
                                                            rshp_id=RSHP_ID,
                                                            prvdr_cd=PRVDR_CD,
                                                            crt_pgm_id=base_file_nm[0])
    custom_logging(TBIPKV_INS_01)
    cur2.executemany(TBIPKV_INS_01, TBIPKV_data)
    conn2.commit()


if __name__ == "__main__":
    # ASK_ID = '2019-A0044' #  #
    # RSHP_ID = 'B0001' #  
    # PRVDR_CD = 'K0004' #  # 
    # SRC_PTH_NM = 'D:/work/2019/out' #  #
    # FILE_NM =  'IF_DL_504_2019A0044_B0001_HN07_ALL_1_1_20210201.txt' #  # 
    # TRUNC_FLAG = 'Y'  

    ASK_ID = sys.argv[1] # '2019-A0044' #
    RSHP_ID = sys.argv[2] # 'B0001' # 
    PRVDR_CD = sys.argv[3] # 'K0004' # 
    SRC_PTH_NM = sys.argv[4] # 'D:/work/2019/out' #
    FILE_NM =  sys.argv[5] # 'IF_DL_504_2019A0044_B0001_HN07_ALL_1_1_201119.txt' # 
    TRUNC_FLAG = sys.argv[6] # 'Y'  

    # 비식별기준이 없으면 파일 생성을 하지 않는다. 비식별만 재작업하기 위하여
    PASS_NOT_EXISTS_TBPINV112 = False  # default = True
    PASS_ALTER_ID = True # default = True

    STDOUT_TRUE = True # default
    STDOUT_FALSE = False
    TBPBTV003_TRUE = True
    TBPBTV003_FALSE = False # default    

    base_file_nm = os.path.basename(__file__).split('.')
    logging.basicConfig(
        filename=LOG_DIR + '/' + base_file_nm[0]+ '_' + datetime.now().strftime('%Y%m%d')   + '.log', \
        level=eval(LOG_LEVEL), filemode='a+', \
        format='{} %(levelname)s : line = %(lineno)d , message = %(message)s'.format(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

    # hmbpdata
    conn2 = tibero_db_conn2()
    cur2 = conn2.cursor()

    main()

    cur2.close()
    conn2.close()


