'''
1. 파일이름 : ProviderFileJob
2. 기능 : 기관별 기관데이터파일을 정합성 검증 & 해시작업을 처리한 후 ESB로 송신한다.
3. 처리절차 : 기관데이터 정합성검증 & 해시작업
    1) 신청정보 상태코드 4296대상 찾기
    2) 기관데이터 파일 탐색
    3) 해시컬럼 유무 비교
      가) 해시컬럼 없을 시 해당 신청정보 TBPPKV005 탐색
      나) 기관데이터에 해시 결합 후 개인정보 삭제
    4) 정합성 검증
    5) 정합성 완료 파일 ESB 송신
    6) 작업결과저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.12.18 배포
    1.01 2020.10.6 소스디렉토리 조정
'''

import os,sys
import shutil
import logging
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.base_agency import *
from base.oracle_dbconn import *
from base.query_sep import *
import pandas as pd
import gc
import datetime
from bin.Consistency import *

#정합성 검증 에러
class custom_checkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

'''
1. 기능 : TBPGPB103 파일 상태 업데이트
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태코드,상태 메세지,신청ID,가설ID,파일명
'''
def TBPGPB103_upt(prcs_fail_yn, prcs_fail_cnts, ask_id,rshp_id, file_nm):
    TBPGPB103_upt_01_src = SQL_DIR + '/' + 'TBPGPB103_upt_01.sql'
    TBPGPB103_upt_01 = query_seperator(TBPGPB103_upt_01_src).format(prcs_fail_yn=prcs_fail_yn,
                                                                    prcs_fail_cnts=prcs_fail_cnts,
                                                                    ask_id = ask_id,
                                                                    rshp_id = rshp_id,
                                                                    file_nm=file_nm)
    cur.execute(TBPGPB103_upt_01)
    conn.commit()


'''
1. 기능 : TBPGPV301 작업로그 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : oid,신청ID,가설ID,상태코드,결과내역,메세지
4. retrun : select 쿼리 결과
'''
def TBPGPV301_ins(oid,ask_id,rshp_id,prcsts,result,content):
    hash_stat_ins_src = SQL_DIR + '/' + 'TBPGPV301_ins_01.sql'
    hash_stat_ins = query_seperator(hash_stat_ins_src).format(
                                                              oid = oid,
                                                              ask_id = ask_id,
                                                              rshp_id = rshp_id,
                                                              wk_tp_cd_nm = '연계데이터 추출작업',
                                                              accessname=base_file_nm[0],
                                                              prcsts = prcsts,
                                                              result = result,
                                                              content=content
                                                              )
    cur.execute(hash_stat_ins)
    conn.commit()


'''
1. 기능 : TBPGPM101 신청정보 업데이트
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태 코드,신청ID,가설ID
4. retrun : select 쿼리 결과
'''
def TBPGPM101_upt_01(prcsts,ask_id,rshp_id):
    TBPGPM101_upt_01_src = SQL_DIR + '/' + 'TBPGPM101_upt_01.sql'
    TBPGPM101_upt_01 = query_seperator(TBPGPM101_upt_01_src).format(prcsts=prcsts,
                                                                    ask_id = ask_id,
                                                                    rshp_id = rshp_id)
    cur.execute(TBPGPM101_upt_01)
    conn.commit()



'''
1. 기능 : 파일 이름 체크
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 신청ID,가설ID,기관코드,데이터셋,파일명
4. retrun : 파일비교 bool값
'''
def File_Naming_Check(ASK_ID,RSHP_ID,PRVDR_CD,CAT_CD,FILE_NM):
    File_Split_Nm = FILE_NM.split('_')
    file_prvdr_num = File_Split_Nm[2]
    file_ask_id = File_Split_Nm[3][:4] + '-' + File_Split_Nm[3][4:]
    file_rshp_id = File_Split_Nm[4]
    if len(File_Split_Nm[5:-3]) > 1 :
        file_cat_cd = '_'.join(File_Split_Nm[5:-3])
    else:
        file_cat_cd = File_Split_Nm[5]
    PR_PG_NUM = File_Split_Nm[-3]
    TOT_PG_NUM = File_Split_Nm[-2]



    logging.info('### File_Naming_Check ###\nfile_ask_id = {} , file_rshp_id = {}, \
    file_prvdr_num = {}'.format(file_ask_id,file_rshp_id,file_prvdr_num))#,file_cat_cd))
    if PRVDR_CD == 'K0001':
        match_prvdr_num = '501'
    elif PRVDR_CD == 'K0002':
        match_prvdr_num = '502'
    elif PRVDR_CD == 'K0003':
        match_prvdr_num = '503'
    else :
        match_prvdr_num = '504'

    return file_prvdr_num == match_prvdr_num and file_ask_id == ASK_ID and file_rshp_id == RSHP_ID and PR_PG_NUM <= TOT_PG_NUM and file_cat_cd == CAT_CD


'''
1. 기능 : 기관데이터 전송 파일명 변경
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 데이터셋명,파일명
4. retrun : 변경된 파일명
'''
def file_naming_change(cat_cd,file_nm):
    file_split_nm = file_nm.split('_')
    if len(file_split_nm[-1]) > 12:
        file_split_nm[-1] = file_split_nm[-1][:8] + '.txt'

    # 데이터셋명 재정의
    if len(file_split_nm[5:-3]) > 1:
        file_split_nm.remove(file_split_nm[5])
    file_split_nm[5] = cat_cd

    tot_file_nm = "_".join(file_split_nm)
    return tot_file_nm

#logging base setup
base_file_nm = os.path.basename(__file__).split('.')
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0]+ '_' + datetime.datetime.now().strftime('%Y%m%d')   + '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')
logging.info('ProviderFileJob program start time == ' +datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

PRCSTS = 4296

conn = oracle_db_conn()
cur = conn.cursor()

#ProviderFile target select
TBPGPM101_sel_01_src = SQL_DIR + '/' + 'TBPGPM101_sel_01.sql'
TBPGPM101_sel_01 = query_seperator(TBPGPM101_sel_01_src).format(prcsts = PRCSTS)
cur.execute(TBPGPM101_sel_01)
TBPGPM101_sel_01_fetchall = cur.fetchall()

if len(TBPGPM101_sel_01_fetchall) == 0 :
    logging.info('신청 상태가 {} 인 대상이 없습니다.'.format(PRCSTS))
    logging.info('ProviderFileJob 종료 == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    sys.exit(1)

#ProviderFile target list != 0 check
for meta_val in TBPGPM101_sel_01_fetchall:
    #meta info columns
    oid = meta_val[0]
    ask_id = meta_val[1]
    # if ask_id[:4] == '2018':
    #     print('ASK_ID = {} => 18년도 과제이므로 생략합니다.'.format(ask_id))
    #     continue
    rshp_id = meta_val[2]
    fail_cnt = 0
    FILE_DF = pd.DataFrame(columns=['PRVDR_CD','CAT_CD','FILE_NM','DI_PRCS_YN'])
    tot_file_nm_list = list()
    err_msg = ''

    #메타파일  정보 딕셔너리
    MetaFile = {}

    try:
        msg = '== OID = {oid}, ASK_ID = {ask_id}, RSHP_ID = {rshp_id} == '.format(oid = oid,
                                                                                                        ask_id = ask_id,
                                                                                                        rshp_id = rshp_id)
        logging.info(msg)
        print(msg)


        target_file_sel_01_src = SQL_DIR + '/' + 'TBPGPB103_sel_01.sql'
        target_file_sel_01 = query_seperator(target_file_sel_01_src).format(ask_id = ask_id,
                                                                            rshp_id =rshp_id)
        cur.execute(target_file_sel_01)
        target_file_sel_01_fetchall = cur.fetchall()

        #해당하는 기관데이터 내역이 없을경우
        if len(target_file_sel_01_fetchall) == 0 :
            err_msg = 'ASK_ID = {ask_id}, RSHP_ID = {rshp_id}에 해당된 기관데이터 내역 없음'.format(ask_id = ask_id,
                                                                                       rshp_id =rshp_id)
            logging.info(err_msg)
            print(err_msg)
            TBPGPV301_ins(oid, ask_id, rshp_id, 4297, '실패', err_msg)
            TBPGPM101_upt_01(4297,ask_id,rshp_id)
            continue

        for target_file_val in target_file_sel_01_fetchall:

            PRVDR_CD = target_file_val[0]
            CAT_CD = target_file_val[1]
            FILE_NM = target_file_val[2]
            DI_PRCS_YN = target_file_val[3]
            file_div_cd = target_file_val[4]

            PROV_IN_DIR = Agency_prov_in_path(PRVDR_CD)

            #파일이 존재하는지 확인
            if os.path.isfile(PROV_IN_DIR + '/' + FILE_NM):
                # File_bool_check = File_Naming_Check(ASK_ID,RSHP_ID,PRVDR_CD,CAT_CD,FILE_NM)

                #메타파일이면 파일정보 저장하고 continue 수행
                if file_div_cd == 'M':
                    MetaFile = {'prvdr_cd': PRVDR_CD, 'file_nm': FILE_NM, 'path': PROV_IN_DIR}
                    continue

                #파일이름 체크 생략시 코드
                TBPGPB103_upt('S', '', ask_id, rshp_id, FILE_NM)
                row = [PRVDR_CD, CAT_CD, FILE_NM, DI_PRCS_YN]
                FILE_DF = FILE_DF.append(pd.Series(row, index=FILE_DF.columns), ignore_index=True)
                logging.info('{} 확인'.format(FILE_NM))
                print('{} 확인'.format(FILE_NM))

                # #파일이름 체크
                # if File_Naming_Check(ask_id, rshp_id, PRVDR_CD, CAT_CD, FILE_NM):
                #         TBPGPB103_upt('S','', ask_id, rshp_id, FILE_NM)
                #         row = [PRVDR_CD,CAT_CD,FILE_NM,DI_PRCS_YN]
                #         FILE_DF = FILE_DF.append(pd.Series(row, index=FILE_DF.columns), ignore_index=True)
                #         logging.info('{} 확인'.format(FILE_NM))
                #         print('{} 확인'.format(FILE_NM))
                # else:
                #     err_msg = ' 이름 형식이 잘못되었습니다.! \n 파일명 :{}\n ASK_ID ={},RSHP_ID={},CAT_CD={}'.format(FILE_NM,
                #                                                                                       ask_id,
                #                                                                                       rshp_id,
                #                                                                                       CAT_CD)
                #     logging.info(err_msg)
                #     print(err_msg)
                #     TBPGPB103_upt('N', err_msg, ask_id, rshp_id, FILE_NM)
                #     fail_cnt += 1
                #     break;
            else:
                err_msg = '파일 없음 : {file_nm}'.format(file_nm = FILE_NM)
                TBPGPB103_upt('N', err_msg, ask_id, rshp_id, FILE_NM)
                #TBPGPV301_ins(OID,ASK_ID,RSHP_ID,'4297','실패',err_msg)
                logging.info(err_msg)
                print(err_msg)
                fail_cnt += 1
                continue

        #실패파일이 1개 이상이면 신청정보 업데이트 후 다음 신청정보 재개
        if fail_cnt != 0:
            err_msg = '기관데이터 파일 {}개 미존재'.format(fail_cnt)
            TBPGPM101_upt_01(4297,ask_id,rshp_id)
            TBPGPV301_ins(oid, ask_id, rshp_id, 4297, '실패', err_msg)
            continue


        for idx in FILE_DF.index:
            PRVDR_CD = FILE_DF._get_value(idx,'PRVDR_CD')
            CAT_CD = FILE_DF._get_value(idx,'CAT_CD')
            FILE_NM = FILE_DF._get_value(idx,'FILE_NM')
            DI_PRCS_YN = FILE_DF._get_value(idx,'DI_PRCS_YN')

            PROV_IN_DIR = Agency_prov_in_path(PRVDR_CD)
            PROV_SAVE_DIR  = Agency_prov_save_path(PRVDR_CD)

            columns = list()


            with open(PROV_IN_DIR + '/' + FILE_NM,encoding='utf-8') as f:
                limit = 1
                count = 0

                for line in f:
                    columns = [word.upper() for word in line[:-1].split(',')]

                    count+=1
                    if count >= limit:
                        break

            #이미 해쉬처리된 파일 혹은 해쉬가 있는 파일은 첫줄 컬럼만 읽고 정합성 검증 후 저장공간으로 이동
            if DI_PRCS_YN == 'Y' or ('HASH_DID' in columns):
                if DI_PRCS_YN == 'Y':
                    msg = '파일 {} DI_PRCS_YN == Y\n정합성 검증 시작'.format(FILE_NM)
                    logging.info(msg)
                    print(msg)
                else:
                    msg = '파일 {} HASH_DID컬럼 존재\n정합성 검증 시작'.format(FILE_NM)
                    logging.info(msg)
                    print(msg)

                #원본데이터 메타데이터 임시 저장
                origin_meta_col_nm = columns[:5]

                #정합성 검증을 위한 4번까지  컬럼 재정의
                columns[:5] = ['ASK_ID','RSHP_ID','PRVDR_CD','HASH_DID','CAT_CD']

                #if not Consistency_check(cur, data_csv1, ask_id, rshp_id, PRVDR_CD, CAT_CD):
                if not Consistency_check(cur, columns, ask_id, rshp_id, PRVDR_CD, CAT_CD):
                    msg = 'File = {file_nm} 정합성 검증 실패'.format(file_nm=FILE_NM)
                    logging.info(msg)
                    TBPGPB103_upt('N', msg, ask_id, rshp_id, FILE_NM)
                    raise custom_checkError(msg)
                else:
                    msg = 'File = {file_nm} 정합성 검증 성공'.format(file_nm=FILE_NM)
                    logging.info(msg)
                    print(msg)


                    #이름변환
                    TOT_FILE_NM = file_naming_change(CAT_CD,FILE_NM)




                    #대문자로 바꾼 컬럼 쓰기
                    f = open(PROV_IN_DIR + '/' + FILE_NM, 'r+',encoding='utf-8')
                    f.write(','.join(columns))
                    f.close()

                    shutil.copy2(PROV_IN_DIR +'/' + FILE_NM,PROV_SAVE_DIR + '/' + TOT_FILE_NM)
                    tot_file_nm_list.append(TOT_FILE_NM)
                    msg = '{file_nm} -> {tot_file_nm} 변환 및 {save_dir} 이동 완료'.format(file_nm = FILE_NM,
                                                                                        tot_file_nm = TOT_FILE_NM,
                                                                                        save_dir = PROV_SAVE_DIR)
                    logging.info(msg)
                    print(msg)
                    continue

            #
            # #컬럼에 해쉬가 존재하면
            # if 'HASH_DID' in data_csv1.columns:
            #     msg = '{} HASH_DID 컬럼 존재. 정합성검증 시작'.format(FILE_NM)
            #     logging.info(msg)
            #     print(msg)
            #     if not Consistency_check(cur, columns, ask_id, rshp_id, PRVDR_CD, CAT_CD):
            #         msg = 'File = {file_nm} 정합성 검증 실패'.format(file_nm=FILE_NM)
            #         logging.info(msg)
            #         TBPGPB103_upt('N', msg, ask_id, rshp_id, FILE_NM)
            #         raise Con_CheckError(msg)
            #


            #해쉬가 존재하지않다면 데이터를 읽어와서 해쉬작업 후 정합성 체크
            else:
                msg = '{} HASH_DID 컬럼 존재하지 않음. 해시작업 시작'.format(FILE_NM)
                logging.info(msg)
                print(msg)
                data_sel_src = SQL_DIR + '/' + 'TBPPKV005_sel_01.sql'
                data_sel = query_seperator(data_sel_src).format(ask_id=ask_id,
                                                                rshp_id=rshp_id,
                                                                prvdr_cd=PRVDR_CD)
                cur.execute(data_sel)
                data005_sel_fetchall = cur.fetchall()

                data005_sel_data = pd.DataFrame(data=data005_sel_fetchall,
                                                columns=['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'HASH_DID', 'MRG_NM',
                                                         'MRG_SSN'])
                ##원본데이터
                # chunk로 분할 read
                chunk = pd.read_csv(PROV_IN_DIR + '/' + FILE_NM, encoding="UTF-8", dtype=str, chunksize=10000,iterator=True)
                data_csv1 = pd.concat([ch for ch in chunk])

                #컬럼명을 대문자로 바꾸기
                # col = [c.upper() for c in data_csv1.columns.tolist()]
                # data_csv1.columns = col
                data_csv1.columns = data_csv1.columns.str.upper()


                #개인정보 이름 컬럼
                mrg_nm_col = data_csv1.columns[3]
                #개인정보 주민번호 컬럼
                mrg_ssn_col = data_csv1.columns[4]
                # 원본 데이터셋명  컬럼 임시 저장
                mrg_dataset = data_csv1.columns[5]

                #이름이 다를수 있으므로 변경
                data_csv1.rename(columns= {mrg_nm_col : 'MRG_NM',
                                           mrg_ssn_col : 'MRG_SSN',
                                           mrg_dataset : 'CAT_CD'},inplace=True)




                merge_data = pd.merge(data_csv1, data005_sel_data, how='inner',
                                      on=['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'MRG_NM', 'MRG_SSN'])

                #TBPPKV005와 머지한 데이터 행수가 0이면 에러발생
                if len(merge_data.index) == 0 :
                    err_msg = '해시작업 실패. 파일데이터와 매치되는 데이터가 없습니다. TBPPKV005 확인'
                    logging.info(err_msg)
                    raise custom_checkError(err_msg)

                # 메타정보 (ask_id, rshp_id, prvdr_cd,hash_did)
                data_meta = merge_data[['ASK_ID', 'RSHP_ID', 'PRVDR_CD','HASH_DID']]

                # 기관제공 변수 (데이터셋,신청 데이터)
                data_val = merge_data[merge_data.columns.difference(['ASK_ID','RSHP_ID','PRVDR_CD',
                                                                     'HASH_DID','MRG_NM', 'MRG_SSN'],sort=False)]
                #data_val = merge_data.iloc[:, 5:-1]
                tot_data = pd.concat([data_meta,data_val], axis=1)#.set_index(['ASK_ID'])

                msg = '{} 해시작업 완료. 정합성검증 시작'.format(FILE_NM)
                logging.info(msg)
                print(msg)
                data_csv1 = tot_data
                # 정합성 검증.. 컬럼 순서, 컬럼명이 db 메타와 file 메타가 같은지 확인
                col = data_csv1.columns.tolist()

                Con_Check = Consistency_check(cur, col, ask_id, rshp_id, PRVDR_CD, CAT_CD)

            if not Con_Check:
                err_msg = 'File = {file_nm} 정합성 검증 실패'.format(file_nm = FILE_NM)
                logging.info(err_msg)
                TBPGPB103_upt('N', err_msg, ask_id, rshp_id, FILE_NM)
                raise custom_checkError(err_msg)
                # TBPGPB103_upt('N',msg,ASK_ID,RSHP_ID,FILE_NM)
                # TBPGPV301_ins(OID,ASK_ID,RSHP_ID,4297,'실패',msg)
                # TBPGPM101_upt_01(4297,OID)

            #아까 바꾼 데이터셋명

            data_csv1 = data_csv1.set_index('ASK_ID')

            TOT_FILE_NM = file_naming_change(CAT_CD, FILE_NM)
            # 기관데이터 저장폴더에 임시 저장
            data_csv1.to_csv(PROV_SAVE_DIR + '/' + TOT_FILE_NM)

            msg = '== {} 정합성 검증 일치 =='.format(FILE_NM)
            logging.info(msg)
            print(msg)
            #컬럼명 모두 대문자로
            # data_csv1.columns = [x.upper() for x in data_csv1.columns]

            msg = '{file_nm} -> {tot_file_nm} 변환 및 {save_dir} 이동 완료'.format(file_nm=FILE_NM,
                                                                            tot_file_nm=TOT_FILE_NM,
                                                                            save_dir=PROV_SAVE_DIR)

            TBPGPB103_upt('S', msg, ask_id, rshp_id, FILE_NM)
            TBPGPV301_ins(oid, ask_id, rshp_id, 4298, '성공', msg)
            tot_file_nm_list.append(TOT_FILE_NM)

            # 만든 데이터프레임 삭제 (메모리 절약)
            del [[chunk,data_csv1]]
            gc.collect()


        #모든파일이 for문을 정상적으로 돌면 해당파일들 전송
        for file_nm in tot_file_nm_list:
            prvdr = file_nm.split('_')[2]

            if prvdr == '501':
                prvdr_cd = 'K0001'
                PROV_SAVE_DIR = Agency_prov_save_path(prvdr_cd)
                PROV_OUT_DIR = Agency_prov_out_path(prvdr_cd)
            elif prvdr == '502':
                prvdr_cd = 'K0002'
                PROV_SAVE_DIR = Agency_prov_save_path(prvdr_cd)
                PROV_OUT_DIR = Agency_prov_out_path(prvdr_cd)
            elif prvdr == '503':
                prvdr_cd = 'K0003'
                PROV_SAVE_DIR = Agency_prov_save_path(prvdr_cd)
                PROV_OUT_DIR = Agency_prov_out_path(prvdr_cd)
            elif prvdr == '504':
                prvdr_cd = 'K0004'
                PROV_SAVE_DIR = Agency_prov_save_path(prvdr_cd)
                PROV_OUT_DIR = Agency_prov_out_path(prvdr_cd)

            shutil.move(PROV_SAVE_DIR + '/' + file_nm, PROV_OUT_DIR +'/' + file_nm)
            msg = '파일 = {file_nm} 전송경로 = {out} 전송완료'.format(file_nm=file_nm,
                                                                  out=PROV_OUT_DIR)
            print(msg)
            logging.info(msg)

        # 모든파일이 전송완료되면 메타파일도 보내기
        send_path = Agency_File_metaout_path(MetaFile.get('prvdr_cd'))
        shutil.move(MetaFile.get('path') + '/' + MetaFile.get('file_nm'), send_path + '/' + MetaFile.get('file_nm'))
        msg = '메타파일 = {file_nm} \n전송경로 = {out} 전송완료'.format(file_nm=MetaFile.get('file_nm'),
                                                                   out=send_path)
        logging.info(msg)
        print(msg)

        TBPGPV301_ins(oid, ask_id, rshp_id, 4298, '성공', '기관데이터 전송 완료')
        TBPGPM101_upt_01(4298,ask_id,rshp_id)

        del [[FILE_DF]]
        gc.collect()

    #정합성 검증 실패 에러
    except custom_checkError as e:
        err_msg = e.msg
        TBPGPM101_upt_01(4297,ask_id,rshp_id)
        TBPGPV301_ins(oid, ask_id, rshp_id, 4297, '실패', err_msg)
        print(err_msg)
    except:
        err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
        err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
        logging.info(err_line +err_msg )
        logging.info('HashJob program end time == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        TBPGPM101_upt_01(4297,ask_id,rshp_id)
        TBPGPV301_ins(oid, ask_id, rshp_id, 4297, '실패', err_msg)

cur.close()
conn.close()