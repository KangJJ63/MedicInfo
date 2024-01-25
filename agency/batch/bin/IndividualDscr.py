'''
1. 파일이름 : IndividualDscr
2. 기능 : 요청정보를 수신받아 요청개인식별정보 데이터를 TBPPKV004에 삽입한다.
3. 처리절차 : 요청정보 결합
    1) 신청정보 상태코드 428대상 찾기
    2) TBPPKV003 데이터와 TBPPKV004 공통 데이터 조회
    3) 공통된 데이터 결합 후 TBPPKV005에 삽입
    4) 작업결과저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.10.5 배포
    1.01 2020.10.6 소스디렉토리 조정
'''

import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.base_agency import *
import datetime
import logging
from base.oracle_dbconn import *
from base.query_sep import *


'''
1. 기능 : TBPGPM101 신청정보 업데이트
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 상태 코드 , 신청ID , 가설 ID
'''
def TBPGPM101_upt_01(prcsts,ask_id,rshp_id):
    TBPGPM101_upt_01_src = SQL_DIR + '/' + 'TBPGPM101_upt_01.sql'
    TBPGPM101_upt_01 = query_seperator(TBPGPM101_upt_01_src).format(prcsts = prcsts,
                                                                    ask_id = ask_id,
                                                                    rshp_id = rshp_id)
    cur.execute(TBPGPM101_upt_01)
    conn.commit()

'''
1. 기능 : TBPGPV301 작업 로그내역 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : oid , 신청ID, 가설ID, 상태콛, 작업결과 , 결과메세지
'''
def TBPGPV301_ins(oid,ask_id,rshp_id,prcsts,result,content):
    hash_stat_ins_src = SQL_DIR + '/' + 'TBPGPV301_ins_01.sql'
    hash_stat_ins = query_seperator(hash_stat_ins_src).format(
                                                              oid = oid,
                                                              ask_id = ask_id,
                                                              rshp_id = rshp_id,
                                                              wk_tp_cd_nm = '요청개인식별정보 작업',
                                                              accessname=base_file_nm[0],
                                                              prcsts = prcsts,
                                                              result = result,
                                                              content=content
                                                              )
    cur.execute(hash_stat_ins)
    conn.commit()

# '''
# 1. 기능 : TBPPKV005 작업 로직
# 2. 최종수정자 : 강재전 2020.11.27
# 3. 파라미터 : 신청ID
# '''
# def TBPPKV005_partion_work(ask_id):
#     ask_id_num = ask_id[:4]+ask_id[5:]
#     try:
#         logging.info('TBPPKV005_{} add 시작'.format(ask_id_num))
#         print('TBPPKV005_{} add 시작'.format(ask_id_num))
#         TBPPKV005_part_add_src = SQL_DIR + '/' + 'TBPPKV005_PART_ADD_01.sql'
#         TBPPKV005_part_add = query_seperator(TBPPKV005_part_add_src).format(ask_id_num=ask_id_num,
#                                                                           ask_id = ask_id)
#         cur.execute(TBPPKV005_part_add)
#         logging.info('TBPPKV005_{} add 완료'.format(ask_id_num))
#         print('TBPPKV005_{} add 완료'.format(ask_id_num))
#     except:
#         msg ='TBPPKV005_{} add 에러! truncate 수행'.format(ask_id_num)
#         print(msg)
#         logging.info(msg)
#
#         TBPPKV005_trc_src = SQL_DIR + '/' + 'TBPPKV005_trc_01.sql'
#         TBPPKV005_trc = query_seperator(TBPPKV005_trc_src).format(ask_id_num = ask_id_num)
#         cur.execute(TBPPKV005_trc)
#
#         msg = 'TBPPKV005_{} truncate 완료. Rebuild 수행'.format(ask_id_num)
#         print(msg)
#         logging.info(msg)
#
#
#         TBPPKV005_reb_src = SQL_DIR + '/' + 'TBPPKV005_REB_01.sql'
#         TBPPKV005_reb = query_seperator(TBPPKV005_reb_src)
#         cur.execute(TBPPKV005_reb)
#         conn.commit()
#
#         msg = 'TBPPKV005 Rebuild 완료'
#         print(msg)
#         logging.info(msg)



base_file_nm = os.path.basename(__file__).split('.')
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0]+ '_' + datetime.datetime.now().strftime('%Y%m%d')+ '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')

logging.info('IndividualDscr program start time == ' +datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

conn = oracle_db_conn()
cur = conn.cursor()
PRCSTS = 428

#target data select
TBPGPM101_sel_01_src = SQL_DIR + '/' + 'TBPGPM101_sel_01.sql'
TBPGPM101_sel_01 = query_seperator(TBPGPM101_sel_01_src).format(prcsts = PRCSTS)
cur.execute(TBPGPM101_sel_01)
TBPGPM101_sel_01_fetchall = cur.fetchall()
logging.info('IndividualDscr target count == ' + str(len(TBPGPM101_sel_01_fetchall)))
#target data != 0
if len(TBPGPM101_sel_01_fetchall) != 0:

    for meta_val in TBPGPM101_sel_01_fetchall:

        # 메타 정보 컬럼 정리
        oid = meta_val[0]
        ask_id = meta_val[1]
        # if ask_id[:4] == '2018':
        #     print('ASK_ID = {} => 18년도 과제이므로 생략합니다.'.format(ask_id))
        #     continue
        rshp_id = meta_val[2]

        msg = '### OID = {oid} ,ASK_ID = {ask_id}, RSHP_ID = {rshp_id} ###'.format(oid=oid,
                                                                                   ask_id=ask_id,
                                                                                   rshp_id=rshp_id)
        logging.info(msg)
        print(msg)
        try:
            #esb 해쉬키 데이터 샘플링 조회 (여부 조회로 1건 조회함)
            TBPPKV004_sel_01_src = SQL_DIR + '/' + 'TBPPKV004_sel_01.sql'
            TBPPKV004_sel_01 = query_seperator(TBPPKV004_sel_01_src).format(ask_id = ask_id,
                                                                            rshp_id = rshp_id)
            cur.execute(TBPPKV004_sel_01)
            TBPPKV004_sel = cur.fetchone()
            fail_cnt = TBPPKV004_sel[0] # 상태 'S'가 아닌 개수
            data_cnt = TBPPKV004_sel[1] # 신청정보에 해당하는 데이터 개수

            # 1. 신청정보에 해당하는 TBPPKV004 데이터들중 송수신상태코드가 'S'(성공)이 아닌 것부터 체크
            msg = "TRNSMRCV_CD 'N'인 개수 : {fail_cnt}개 ,\nTBPPKV004's counts matched in TBPPKV003 : {data_cnt}개 ".format(fail_cnt =fail_cnt,
                                                                           data_cnt = data_cnt)
            logging.info(msg)
            print(msg)

            if fail_cnt != 0 :
                TBPGPV301_ins(oid, ask_id, rshp_id, 428, '실패', 'TBPPKV004 데이터 송수신상태 확인')
                # 신청정보 상태 변경
                TBPGPM101_upt_01(PRCSTS,ask_id,rshp_id)
                err_msg = 'TBPPKV004 송수신 상태 에러 개수 : {}개'.format(fail_cnt)
                logging.info(err_msg)
                print(err_msg)
                continue



            if data_cnt != 0:
                #기존 최종 테이블에 동일 데이터가 있을시 삭제
                # TBPPKV005_partion_work(ask_id)


                # TBPPKV005 delete 하기
                TBPPKV005_del_src = SQL_DIR + '/' + 'TBPPKV005_del_01.sql'
                TBPPKV005_del = query_seperator(TBPPKV005_del_src).format(ask_id=ask_id,
                                                                          rshp_id=rshp_id)
                cur.execute(TBPPKV005_del)

                msg = 'ASK_ID = {}, RSHP_ID = {} 의 TBPPKV003 데이터 삭제 완료.'.format(ask_id, rshp_id)
                print(msg)
                logging.info(msg)




                # 최종테이블 입력 (요청정보와 기관해시키는 DB에 들어가있음으로 조인하여 데이터 입력)
                disc_ins_src = SQL_DIR + '/' + 'TBPPKV005_ins_01.sql'
                disc_ins = query_seperator(disc_ins_src).format(ask_id = ask_id,
                                                                rshp_id = rshp_id,
                                                                crt_pgm_id = base_file_nm[0])
                cur.execute(disc_ins)
                conn.commit()
                logging.info('TBPPKV005_ins_01 성공..')

                # 고객 데이터를 추출하기 위해 상태값 변경
                TBPGPM101_upt_01(429,ask_id,rshp_id)
                logging.info('TBPGPM101_upt_01 success..')
                print('TBPPKV005_ins_01 성공..')

                # 이력 테이블 상태값 변경
                TBPGPV301_ins(oid, ask_id, rshp_id, 429, '성공', 'TBPPKV005 적재 완료')

                logging.info('IndividualDscr_hist_ins_01 success..')
                logging.info('IndividualDscr program end time == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))


            else:
                # 로그 기록
                TBPGPV301_ins(oid, ask_id, rshp_id, 428, '실패', 'TBPPKV003 not matched')
                #신청정보 상태 변경
                TBPGPM101_upt_01(PRCSTS,ask_id,rshp_id)
                err_msg = 'ASK_ID = {ask_id} RSHP_ID = {rshp_id} not matched in TBPPKV004'.format(ask_id=ask_id,
                                                                                                  rshp_id = rshp_id)
                logging.info(err_msg)
                print(err_msg)
                logging.info('IndividualDscr program end time == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        except:
            # 조회 테이블 상태값 변경 및 이력 테이블 상태값 변경 및 오류 메시지 db 입력
            err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
            err_msg = '  err_msg == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')

            #신청정보 상태 변경
            TBPGPM101_upt_01(PRCSTS,ask_id,rshp_id)
            #로그 기록
            TBPGPV301_ins(oid, ask_id, rshp_id, 428, '실패', err_msg)
            logging.error(err_line +err_msg)
            print(err_msg)
            logging.info('IndividualDscr program end time == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

else:
    logging.info('상태코드가 428인 신청정보가 존재하지 않습니다.')
    logging.info('IndividualDscr program end time == ' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

cur.close()
conn.close()
