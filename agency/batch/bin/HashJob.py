'''
1. 파일이름 : HashJob
2. 기능 : 기관별 개인식별정보파일에 해시를 처리하여 기관 해시키를 생성한다.
3. 처리절차 : 해시작업
    1) 신청정보 상태코드 4266대상 찾기
    2) 개인식별정보 파일 탐색
    3) 해시작업 유무 비교
    4) 성명/주민번호 해시 처리
    5) 기관결합키 / 기관 해시키 생성
    6) 작업결과저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.10.5 배포
    1.01 2020.10.6 소스디렉토리 조정
'''



import os
import pandas as pd
import hashlib
import datetime
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.base_agency import *
import logging
from base.oracle_dbconn import *
from base.query_sep import *
import shutil
import time
import gc


class Hash_columns_CheckError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

'''
1. 기능 : TBPGPM101 신청정보 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태 코드 (prcsts)
4. retrun : select 쿼리 결과
'''
def TBPGPM101_sel_01(prcsts):
    hash_target_sel_01_src = SQL_DIR + '/' + 'TBPGPM101_sel_01.sql'
    hash_target_sel_01 = query_seperator(hash_target_sel_01_src).format(prcsts = prcsts)
    cur.execute(hash_target_sel_01)
    return cur.fetchall()

'''
1. 기능 : TBPGPB102 파일 상태 업데이트
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 파일 실패 여부, 파일 에러메세지 , 파일명 
'''
#TBPGPB102 상태 업데이트 함수
def TBPGPB102_upt(PRCS_FAIL_YN,PRCS_FAIL_CNTS,OID,FILE_NM):
    filestat_upt_stat_01_src = SQL_DIR + '/' + 'TBPGPB102_upt_01.sql'
    filestat_upt_stat_01 = query_seperator(filestat_upt_stat_01_src).format(prcs_fail_yn = PRCS_FAIL_YN,
                                                                            prcs_fail_cnts = PRCS_FAIL_CNTS,
                                                                            oid = OID,
                                                                            file_nm = FILE_NM)
    cur.execute(filestat_upt_stat_01)
    conn.commit()

'''
1. 기능 : TBPGPV301 작업 로그 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : oid,신청ID,가설ID,상태코드,결과코드,결과메세지
'''
#연계처리 로그내역 입력 함수
def TBPGPV301_ins(oid,ask_id,rshp_id,prcsts,result,content):
    hash_stat_ins_src = SQL_DIR + '/' + 'TBPGPV301_ins_01.sql'
    hash_stat_ins = query_seperator(hash_stat_ins_src).format(
                                                              oid = oid,
                                                              ask_id = ask_id,
                                                              rshp_id = rshp_id,
                                                              wk_tp_cd_nm = '해쉬 작업',
                                                              accessname=py_file_nm,
                                                              prcsts = prcsts,
                                                              result = result,
                                                              content=content
                                                              )
    cur.execute(hash_stat_ins)
    conn.commit()


'''
1. 기능 : TBPGPB102 파일 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 신청ID,가설ID
4. retrun : select 쿼리 결과
'''

def TBPGPB102_sel_01 (ASK_ID,RSHP_ID):
    TBPGPB102_sel_01_src = SQL_DIR + '/' + 'TBPGPB102_sel_01.sql'
    TBPGPB102_sel_01 = query_seperator(TBPGPB102_sel_01_src).format(ask_id = ASK_ID,
                                                                    rshp_id = RSHP_ID)
    cur.execute(TBPGPB102_sel_01)
    TBPGPB102_sel_01_fetchall = cur.fetchall()
    return TBPGPB102_sel_01_fetchall



'''
1. 기능 : TBPGPM101 상태 업데이트
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태코드,신청ID,가설ID
'''
def TBPGPM101_upt_01(prcsts,ask_id,rshp_id):
    TBPGPM101_upt_01_src = SQL_DIR + '/' + 'TBPGPM101_upt_01.sql'
    TBPGPM101_upt_01 = query_seperator(TBPGPM101_upt_01_src).format(prcsts = prcsts,
                                                                    ask_id = ask_id,
                                                                    rshp_id = rshp_id)
    cur.execute(TBPGPM101_upt_01)
    conn.commit()

'''
1. 기능 : 기관해시키 파일인터페이스명 변환 함수
2. 최종수정자 : 강재전 2020.11.17
3. 파라미터 : 기관코드
4. retrun : 파일 인터페이스명
'''
def Hash_Interface_Naming(prvdr_cd):
    if prvdr_cd == 'K0001':
        return 'IF_DL_301_'
    elif prvdr_cd == 'K0002' :
        return 'IF_DL_302_'
    elif prvdr_cd == 'K0003':
        return 'IF_DL_303_'
    else:
        return 'IF_DL_304_'


# '''
# 1. 기능 : TBPPKV003 파티션 작업 로직
# 2. 최종수정자 : 강재전 2020.12.07
# 3. 파라미터 : 신청ID
# '''
# def TBPPKV003_partion_work(ask_id):
#     ask_id_num = ask_id[:4]+ask_id[5:]
#     try:
#         logging.info('TBPPKV003_{} add 시작'.format(ask_id_num))
#         print('TBPPKV003_{} add 시작'.format(ask_id_num))
#         TBPPKV003_part_add_src = SQL_DIR + '/' + 'TBPPKV003_PART_ADD_01.sql'
#         TBPPKV003_part_add = query_seperator(TBPPKV003_part_add_src).format(ask_id_num=ask_id_num,
#                                                                           ask_id = ask_id)
#         cur.execute(TBPPKV003_part_add)
#         logging.info('TBPPKV003_{} add 완료'.format(ask_id_num))
#         print('TBPPKV003_{} add 완료'.format(ask_id_num))
#     except:
#         msg ='TBPPKV003_{} add 에러! truncate 수행'.format(ask_id_num)
#         print(msg)
#         logging.info(msg)
#
#         TBPPKV003_trc_src = SQL_DIR + '/' + 'TBPPKV003_trc_01.sql'
#         TBPPKV003_trc = query_seperator(TBPPKV003_trc_src).format(ask_id_num = ask_id_num)
#         cur.execute(TBPPKV003_trc)
#
#         msg = 'TBPPKV003_{} truncate 완료. Rebuild 수행'.format(ask_id_num)
#         print(msg)
#         logging.info(msg)
#
#         TBPPKV003_reb_src = SQL_DIR + '/' + 'TBPPKV003_REB_01.sql'
#         TBPPKV003_reb = query_seperator(TBPPKV003_reb_src)
#         cur.execute(TBPPKV003_reb)
#         conn.commit()
#
#         msg = 'TBPPKV003 Rebuild 완료'
#         print(msg)
#         logging.info(msg)


'''
1. 기능 : 개인식별정보 컬럼명 로드
2. 최종수정자 : 강재전 2020.11.27
3. 파라미터 : 파일 경로
4. retrun : 파일컬럼명
'''
def File_read_col(path):
    with open(path,'r',encoding="UTF-8") as f:
        columns = f.readlines(1)
    return columns[0][:-1].upper()




#현재 파일명
base_file_nm = os.path.basename(__file__).split('.')
py_file_nm = base_file_nm[0]

#로그 설정
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0]+ '_' + datetime.datetime.now().strftime('%Y%m%d')+ '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')

logging.info('HashJob program start time == ' +datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

conn = oracle_db_conn()
cur = conn.cursor()

#신청 상태 코드
PRCSTS = 4266


# 해쉬 처리 대상 신청 정보 조회 결과를 db_sel_info_3에 저장
hash_target_sel_01_fetchall = TBPGPM101_sel_01(PRCSTS)

logging.info('HashJob target count == ' +str(len(hash_target_sel_01_fetchall)))

if len(hash_target_sel_01_fetchall) == 0 :
    logging.info('신청정보 상태코드가 {}인 대상이 없습니다.'.format(PRCSTS))
    logging.info('HashJob program end time == ' +datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sys.exit(1)



#신청정보별 처리
for meta_val in hash_target_sel_01_fetchall:

    try:
        oid = meta_val[0]
        ask_id = meta_val[1]
        rshp_id = meta_val[2]
        #실패파일 개수
        Fail_File_cnt = 0
        #파일 조회 저장용 데이터프레임

        FILE_DF = pd.DataFrame(columns=['PRVDR_CD','FILE_NM','HASHFILE_IN_DIR','DI_PRCS_YN'])

        #메타정보파일 저장용 딕셔너리
        MetaFile = {}




        #최종성공한 후 보낼 파일 목록 리스트
        TOT_FILE_NM_LIST = list()

        msg = '### OID = {oid} ,ASK_ID = {ask_id}, RSHP_ID = {rshp_id} ###'.format(oid = oid,
                                                                                   ask_id = ask_id,
                                                                                   rshp_id = rshp_id)
        logging.info(msg)
        print(msg)


        #신청정보에 따른 TBPGPB102에 파일내역이 있는지 확인
        target_file_sel_01 = TBPGPB102_sel_01(ask_id, rshp_id)

        #신청정보에 따른 파일 내역이 TBPGPB102에 없으면 continue
        if len(target_file_sel_01) == 0:
            # 파일 존재 X 로그
            err_msg= '{} 에 대한 TBPGPB102 파일내역이 없습니다.'.format(oid)
            logging.info(err_msg)
            TBPGPM101_upt_01(4267, ask_id,rshp_id)
            TBPGPV301_ins(oid, ask_id, rshp_id, 4267, '실패', err_msg)
            continue

        #실제 파일이 있는지 여부 체크
        for Target_val in target_file_sel_01:
            prvdr_cd = Target_val[0]
            file_nm = Target_val[1]
            di_prcs_yn = Target_val[2]
            file_div_cd = Target_val[3]

            #기관에 따른 개인식별정보 데이터 경로(메타파일도 똑같은 경로)
            HASH_IN_DIR = Agency_hash_in_path(prvdr_cd)

            #메타파일인지 확인 후 메타파일이면 따로 변수에 저장
            if file_div_cd == 'M':
                MetaFile = {'prvdr_cd':prvdr_cd,'file_nm':file_nm,'path':HASH_IN_DIR}
                continue

            #파일이 존재한다면 ( 추후에 경로 변경 해야함)
            if os.path.isfile(HASH_IN_DIR + '/' + file_nm):
                #파일이름 체크
                File_Split_Nm = file_nm.split('_')
                file_ask_id = File_Split_Nm[0][:4]+'-'+File_Split_Nm[0][4:]
                file_rshp_id = File_Split_Nm[1]
                file_nm_length = len(File_Split_Nm)
                PR_PG_NUM = File_Split_Nm[-3]
                TOT_PG_NUM = File_Split_Nm[-2]

                #파일이름 생략 시 주석 코드
                TBPGPB102_upt('S', '', oid, file_nm)
                row = [prvdr_cd, file_nm, HASH_IN_DIR, di_prcs_yn]
                FILE_DF = FILE_DF.append(pd.Series(row, index=FILE_DF.columns), ignore_index=True)
                logging.info('{} 확인'.format(file_nm))
                print('{} 확인'.format(file_nm))

            #파일이 존재하지 않는다면
            else:
                err_msg = '{} 파일이 없습니다.'.format(file_nm)
                logging.info('에러 : '+ err_msg)
                print(err_msg)
                TBPGPB102_upt('N', err_msg, oid, file_nm)
                Fail_File_cnt += 1
                break;
        #실패파일이 1개 이상이면 신청정보 업데이트 후 다음 신청정보 재개
        if Fail_File_cnt != 0:
            TBPGPM101_upt_01(4267,ask_id,rshp_id)
            TBPGPV301_ins(oid, ask_id, rshp_id, 4267, '실패', err_msg)
            continue

        # #PKV003 파티션 추가 함수
        # TBPPKV003_partion_work(ask_id)

        #TBPPKV003 delete 하기
        TBPPKV003_del_src = SQL_DIR + '/' + 'TBPPKV003_del_01.sql'
        TBPPKV003_del = query_seperator(TBPPKV003_del_src).format(ask_id = ask_id,
                                                                  rshp_id = rshp_id)
        cur.execute(TBPPKV003_del)

        msg = 'ASK_ID = {}, RSHP_ID = {} 의 TBPPKV003 데이터 삭제 완료.'.format(ask_id,rshp_id)
        print(msg)
        logging.info(msg)

        #정상 파일이 모두 존재한다면 저장했던 데이터프레임으로 해쉬작업 실시
        for idx in FILE_DF.index:
            prvdr_cd = FILE_DF._get_value(idx, 'PRVDR_CD')
            file_nm = FILE_DF._get_value(idx, 'FILE_NM')
            HASHFILE_IN_DIR = FILE_DF._get_value(idx,'HASHFILE_IN_DIR')
            di_prcs_yn = FILE_DF._get_value(idx, 'DI_PRCS_YN')

            HASH_SAVE_DIR = Agency_hash_save_path(prvdr_cd)
            #HASH_OUT_DIR = Agency_hash_out_path(PRVDR_CD)

            # 1. 첫줄에 컬럼이 있는지 확인
            meta_col_nm = ['ASK_ID','RSHP_ID','PRVDR_CD']
            ori_file_col = File_read_col(HASH_IN_DIR + '/' + file_nm)
            ori_file_col = ori_file_col.split(',')

            for col in meta_col_nm:
                if col not in ori_file_col:
                    err_msg = '파일 {file_nm} 컬럼명 없음'.format(file_nm=file_nm)
                    logging.info(err_msg)
                    raise Hash_columns_CheckError(err_msg)


            #비식별 된것은 이름만 바꾸고 비식별 파일 저장공간으로 전달
            if di_prcs_yn == 'Y':
                msg = "파일 {} DI_PRCS_YN ='Y' ! 컬럼 체크 시작"
                print(msg)
                logging.info(msg)

                file_col = File_read_col(HASH_IN_DIR + '/' + file_nm)
                file_col = file_col.split(',')
                match_columns = ['ASK_ID','RSHP_ID','PRVDR_CD','HASH_DID']

                #match 컬럼이 있는지 조사
                if len(file_col) == 4:
                    for col in file_col:
                        if col not in match_columns:
                            err_msg = '파일 {file_nm}\n기관결합키에 맞지않는 컬럼 존재 => {col}'.format(file_nm = file_nm,
                                                                                        col = col)
                            logging.info(err_msg)
                            raise Hash_columns_CheckError(err_msg)
                else:
                    err_msg = '파일 {file_nm} 컬럼 개수 에러 ( col > 4 )'.format(file_nm=file_nm)
                    logging.info(err_msg)
                    raise Hash_columns_CheckError(err_msg)

                f = open(HASH_IN_DIR + '/' + file_nm, 'r+', encoding='utf-8')
                f.write(','.join(file_col))
                f.close()

                interface_nm = Hash_Interface_Naming(prvdr_cd)
                TOT_FILE_NM = interface_nm + file_nm
                shutil.copy2(HASHFILE_IN_DIR +'/' + file_nm, HASH_SAVE_DIR + '/' + TOT_FILE_NM)
                TOT_FILE_NM_LIST.append(TOT_FILE_NM)
                msg = '컬럼 체크 완료.\n{file_nm} => {tot_file_nm}변환. \n{save_dir} 이동 완료'.format(file_nm = file_nm,
                                                                                            tot_file_nm = TOT_FILE_NM,
                                                                                            save_dir = HASH_SAVE_DIR)
                logging.info(msg)
                print(msg)
                TBPGPV301_ins(oid,ask_id,rshp_id,4268,'성공',msg)
                continue

            #파일불러오기

            chunk_df = pd.read_csv(HASHFILE_IN_DIR + '/' + file_nm, encoding='utf-8', dtype='category',
                                   chunksize=10000, iterator=True)
            ori_csv_file = pd.concat([ch for ch in chunk_df])

            #소문자가 있을경우 모두 대문자로

            # 메타정보와 개인정보만을 추출
            pd_file_csv = ori_csv_file.iloc[:,0:5]
            #핸들링을 위해 컬럼 재정의
            pd_file_csv.columns = ['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'MRG_NM', 'MRG_SSN']



            # TTP 대상 테이블 조회
            ttp_sel_src = SQL_DIR + '/' + 'TBPPKV001_sel_01.sql'
            ttp_sel = query_seperator(ttp_sel_src).format(ask_id=ask_id,
                                                          rshp_id=rshp_id,
                                                          prvdr_cd=prvdr_cd)
            cur.execute(ttp_sel)
            ttp_sel_fetchall = cur.fetchall()
            if len(ttp_sel_fetchall)==0:
                err_msg = 'ASK_ID = {} , RSHP_ID = {}에 해당하는 TTP 조회결과가 없습니다.'.format(ask_id, rshp_id)
                print(err_msg)
                logging.info(err_msg)
                raise Exception(err_msg)

            msg = 'TTP = {}'.format(ttp_sel_fetchall[0][3])
            print(msg)
            logging.info(msg)
            ttp_pandas = pd.DataFrame(data=ttp_sel_fetchall, columns=['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'TTP_ID'])



            # TTP 대상과 실제 대상 합치고, 해쉬 적용 (sha256 적용 , encodig = utf8)
            pd_data_merge = pd.merge(pd_file_csv, ttp_pandas, how='inner', on=['ASK_ID', 'RSHP_ID', 'PRVDR_CD'])
            pd_data_merge['HASH_DID'] = [hashlib.sha256(xx).hexdigest().upper() for xx in (pd_data_merge['MRG_NM'].astype(str) + \
                                                                                           pd_data_merge['MRG_SSN'].astype(str) + \
                                                                                           pd_data_merge[
                                                                                               'TTP_ID']).str.encode(
                encoding='utf8')]


            logging.info('ttp 및 해쉬 적용 완료 파일 == {file_nm}'.format(file_nm = file_nm))



            # 1차 중복제거 수행
            pd_data_merge = pd_data_merge.drop_duplicates()


            # TBPPKV003 Merge
            pd_data_to_list = pd_data_merge.values.tolist()
            hash_tmp_ins_src = SQL_DIR + '/' + 'TBPPKV003_ins_01.sql'
            hash_tmp_ins = query_seperator(hash_tmp_ins_src)
            cur.executemany(hash_tmp_ins, pd_data_to_list)
            conn.commit()

            logging.info('TBPPKV003입력 완료. file_nm == ' + file_nm)
            print('{} 해쉬작업 완료'.format(file_nm))
            TBPGPV301_ins(oid, ask_id, rshp_id, 4268, '성공', '{} 해쉬작업 완료'.format(file_nm))

            #기관결합키 송신파일 만들기
            out_data = pd_data_merge[['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'HASH_DID']]
            interface_nm = Hash_Interface_Naming(prvdr_cd)
            TOT_FILE_NM = interface_nm + file_nm
            logging.info('{file_nm} 이름 변환 => {TOT_FILE_NM}'.format(file_nm = file_nm,
                                                                   TOT_FILE_NM = TOT_FILE_NM))

            #송신파일 저장공간에 적재
            out_data.set_index('ASK_ID').to_csv(HASH_SAVE_DIR + '/' + TOT_FILE_NM)
            msg = '{TOT_FILE_NM}파일 {path}에 적재 완료'.format(TOT_FILE_NM = TOT_FILE_NM,
                                                                    path = HASH_SAVE_DIR)
            logging.info(msg)
            print(msg)

            #최종 파일이름 저장
            TOT_FILE_NM_LIST.append(TOT_FILE_NM)

            #만든 데이터프레임 삭제 (메모리 절약)
            del [[chunk_df,ori_csv_file]]


        #에러 없이 성공하면 TOT_FILE_NM_LIST에 저장된 파일들 전송폴더로 옮기기
        for file_nm in TOT_FILE_NM_LIST:
            prvdr = file_nm.split('_')[2]
            #
            if prvdr == '301':
                prvdr_cd = 'K0001'
                HASH_SAVE_DIR = Agency_hash_save_path(prvdr_cd)
                HASH_OUT_DIR = Agency_hash_out_path(prvdr_cd)
            elif prvdr == '302':
                prvdr_cd = 'K0002'
                HASH_SAVE_DIR = Agency_hash_save_path(prvdr_cd)
                HASH_OUT_DIR = Agency_hash_out_path(prvdr_cd)
            elif prvdr == '303':
                prvdr_cd = 'K0003'
                HASH_SAVE_DIR = Agency_hash_save_path(prvdr_cd)
                HASH_OUT_DIR = Agency_hash_out_path(prvdr_cd)
            else :
                prvdr_cd = 'K0004'
                HASH_SAVE_DIR = Agency_hash_save_path(prvdr_cd)
                HASH_OUT_DIR = Agency_hash_out_path(prvdr_cd)

            shutil.move(HASH_SAVE_DIR+'/'+file_nm,HASH_OUT_DIR +'/'+file_nm)
            msg='파일 = {file_nm} \n전송경로 = {out} 전송완료'.format(file_nm = file_nm,
                                                                  out = HASH_OUT_DIR)
            print(msg)
            logging.info(msg)

        #모든파일이 전송완료되면 메타파일도 보내기
        send_path = Agency_File_metaout_path(MetaFile.get('prvdr_cd'))
        shutil.move(MetaFile.get('path') + '/' + MetaFile.get('file_nm'), send_path + '/' + MetaFile.get('file_nm'))
        msg = '메타파일 = {file_nm} \n전송경로 = {out} 전송완료'.format(file_nm = MetaFile.get('file_nm'),
                                                                   out = send_path)
        logging.info(msg)
        print(msg)

        #딜레이
        time.sleep(1)
        TBPGPV301_ins(oid, ask_id, rshp_id, 4268, '성공', '해쉬 전송 완료')
        TBPGPM101_upt_01(4268,ask_id,rshp_id)

        del [[FILE_DF]]

    except Hash_columns_CheckError as e :
        print(e)
        err_msg = e.msg
        TBPGPM101_upt_01(4267,ask_id,rshp_id)
        TBPGPV301_ins(oid, ask_id, rshp_id, 4267, '실패', err_msg)
    except:
        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
        err_msg = ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        print(err_line,err_msg)
        logging.info(err_line +'  err_msg == ' +err_msg )
        logging.info('HashJob program end time == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        TBPGPM101_upt_01(4267,ask_id,rshp_id)
        TBPGPV301_ins(oid, ask_id, rshp_id, 4267, '실패', err_msg)
cur.close()
conn.close()
