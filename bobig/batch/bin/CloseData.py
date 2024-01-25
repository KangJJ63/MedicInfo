'''
1. 파일이름 : CloseData
2. 기능 : 비식별데이터를 폐쇄망으로 전송한다.
3. 처리절차 : 비식별 작업
    1) 상세작업구분(85:폐쇄망 전송) 배치(TBPBTV001)등록 성공한 배치실행(TBPBTV002) 없음
    2) 데이터파일찾기
    3) 폐쇄망 경로로 파일 move
    4) 작업결과저장
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
from datetime import datetime
import shutil
import time

class custom_checkError(Exception):
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
def TBPBTV001_sel():

    tbpbtv001_target_sel_01_src = SQL_DIR + '/' + 'TBPBTV001_SEL_01.sql'
    tbpbtv001_target_sel_01 \
        = query_seperator(tbpbtv001_target_sel_01_src).format(wk_dtl_tp_cd = WK_DTL_TP_CD,
                                                              bt_seq = BT_SEQ,
                                                              crt_pgm_id = base_file_nm[0])
    cur.execute(tbpbtv001_target_sel_01)
    target_sel= cur.fetchall()
    return target_sel

'''
1. 기능 : TBPBTV002 결과 내역 최초 입력 
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 배치 등록 번호
4. retrun : 배치 결과 내역 등록번호
'''
def TBPBTV002_ins(file_nm):
    TBPBTV002_ins_01_src = SQL_DIR + '/' + 'TBPBTV002_ins_01.sql'
    TBPBTV002_ins_01 = query_seperator(TBPBTV002_ins_01_src).format(bt_seq=BT_SEQ,
                                                                    crt_pgm_id=file_nm)

    cur.execute(TBPBTV002_ins_01)
    conn.commit()

    # BT_EXEC_SEQ 확인
    TBPBTV002_sel_02_src = SQL_DIR + '/' + 'TBPBTV002_sel_01.sql'
    TBPBTV002_sel_02 = query_seperator(TBPBTV002_sel_02_src).format(bt_seq=BT_SEQ)

    cur.execute(TBPBTV002_sel_02)
    batch_sel_rec = cur.fetchone()
    return int(batch_sel_rec[0])


'''
1. 기능 : TBPINB103 연계데이터 파일내역 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 신청ID, 가설ID
4. retrun : 파일 내역 조회 결과
'''
def TBPINB103_sel(ask_id,rshp_id):
    TBPINB103_sel_03_src = SQL_DIR +'/' + 'TBPINB103_SEL_01.sql'
    TBPINB103_sel_03 = query_seperator(TBPINB103_sel_03_src).format(ask_id = ask_id,
                                                                    rshp_id = rshp_id)
    cur.execute(TBPINB103_sel_03)
    target_file_fetchall = cur.fetchall()
    return target_file_fetchall

'''
1. 기능 : TBPBTV003(배치상세결과내역) 저장
2. 최종수정자 : 강재전 2020.10.5
3. 파라미터 : 상태코드 , 메세지
'''
def TBPBTV003_ins(wk_exec_sts_cd, wk_exec_cnts):
    global BT_SEQ,BT_EXEC_SEQ

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
1. 기능 : TBPBTV003 상태코드 조회
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 
'''
def TBPBTV003_sel():
    global BT_SEQ,BT_EXEC_SEQ
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
            TBPBTV002_upt('003', '파이썬 작업 성공', '비식별 파일 폐쇄망 전송 완료')
        else:
            TBPBTV002_upt('002','파이썬 작업 성공','비식별 파일 폐쇄망 전송 완료')
    else:
        TBPBTV002_upt('902', '실패', '비식별 파일 폐쇄망 전송 실패. 파일 에러 개수 = {}개'.format(fail_cnt))

'''
1. 기능 : TBPINV114 작업 로그 입력
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 신청ID,가설ID,기관코드,데이터셋,실행번호,로그 메세지,파일명
'''
def TBPINV114_ins_01(ask_id,rshp_id,prvdr_cd,cat_cd,exec_seq,wk_exec_cnts_file_nm):
    TBPINV114_INS_01_src = SQL_DIR + '/' + 'TBPINV114_INS_01.sql'
    TBPINV114_INS_01 = query_seperator(TBPINV114_INS_01_src).format(ask_id = ask_id,
                                                                    rshp_id = rshp_id,
                                                                    prvdr_cd = prvdr_cd,
                                                                    cat_cd=cat_cd,
                                                                    exec_seq=exec_seq,
                                                                    di_wk_div_cnts='85',
                                                                    wk_exec_cnts= wk_exec_cnts_file_nm+' 폐쇄망전송 완료',
                                                                    crt_pgm_id=base_file_nm[0])
    logging.info('TBPINV114_INS_01 {} == {}'.format(TBPINV114_INS_01,
                                                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    cur.execute(TBPINV114_INS_01)


'''
1. 기능 : TBPBTV002(배치상세결과내역) 작업 결과 갱신 
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 성공 여부 , 에러 구문, 배치결과등록번호
'''
# 8. TBPBTV003 총 실패여부에 따른 TBPBTV002 결과내역 업데이트 함수
def TBPBTV002_upt(wk_sts_cd, wk_rslt_cnts, wk_rslt_dtl_cnts):
    global BT_SEQ,BT_EXEC_SEQ
    cur = conn.cursor()
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

def main():
    close_data_target = TBPBTV001_sel()

    #배치대상이 있으면 로직 수행
    if len(close_data_target) != 0 :
        global BT_SEQ,BT_EXEC_SEQ
        target_val = close_data_target[0]
        try:
            ask_id = target_val[0]
            # if ask_id[:4] == '2018':
            #     err_msg = 'ASK_ID = {} => 18년도 과제이므로 생략합니다.'.format(ask_id)
            #     raise custom_checkError(err_msg)
            rshp_id = target_val[1]
            #BT_SEQ = target_val[3]
            BT_EXEC_SEQ = TBPBTV002_ins(base_file_nm[0])
            if target_val[2] is None:
                err_msg = '배치등록정보에 실행일련번호(EXEC_SEQ)가 없습니다.'
                raise custom_checkError(err_msg)
            exec_seq = target_val[2]
            cnorgcode = target_val[3]
            suc_cnt=0
            fail_cnt = 0 #파일이 없는 횟수

            if target_val[5] is not None:
                bobig_deid_in_data = target_val[5]
            else:
                bobig_deid_in_data = Bobig_deid_path(cnorgcode)

            if target_val[6] is not None:
                RSLT_PTH_NM = target_val[6]
            else:
                RSLT_PTH_NM = Bobig_move_deid_data_path(cnorgcode)

            msg = 'ASK_ID = {ask_id}, RSHP_ID = {rshp_id}, BT_SEQ = {bt_seq}, EXEC_SEQ = {exec_seq}'.format(ask_id = ask_id,
                                                                                                            rshp_id = rshp_id,
                                                                                                            bt_seq = BT_SEQ,
                                                                                                            exec_seq = exec_seq)
            print('####################\n',msg,'#################\n')
            logging.info(msg)
            datafile_fetchall = TBPINB103_sel(ask_id,rshp_id)

            #TBPINB103 데이터프레임으로 저장
            FILE_DF = pd.DataFrame(columns=['PRVDR_CD','CAT_CD','FILE_NM'])
            #연계데이터 결과파일 가져오기
            if len(datafile_fetchall) != 0:
                for datafile_val in datafile_fetchall:
                    PRVDR_CD = datafile_val[0]
                    CAT_CD = datafile_val[1]
                    FILE_TEMP_NM = datafile_val[2]
                    FILE_SPLIT_NM = FILE_TEMP_NM.split('_')

                    #고전적비식별화된 파일을 찾기 위해 이름 변환
                    if cnorgcode == 'K0001':
                        FILE_SPLIT_NM[2] = '801'
                    elif cnorgcode in ('K0002','K0003','K0004'):
                        FILE_SPLIT_NM[2] = '802'
                    else:
                        #오류
                        print('잘못된 형식의 파일')
                        fail_cnt+=1

                    if len(FILE_SPLIT_NM[-1]) >12:
                        FILE_SPLIT_NM[-1] = FILE_SPLIT_NM[-1][:8] +'.txt'

                    #데이터셋명 재정의
                    if len(FILE_SPLIT_NM[5:-3]) >1 :
                        for i in range(len(FILE_SPLIT_NM[5:-3]) - 1):
                            FILE_SPLIT_NM.remove(FILE_SPLIT_NM[5])
                    FILE_SPLIT_NM[5] = CAT_CD

                    TOT_FILE_NM = "_".join(FILE_SPLIT_NM)

                    if not os.path.isfile(bobig_deid_in_data + '/'+TOT_FILE_NM):
                        err_msg = '{} 파일이 존재하지 않습니다.'.format(TOT_FILE_NM)
                        logging.info(err_msg)
                        fail_cnt +=1
                        TBPBTV003_ins('902',err_msg)
                        continue

                    logging.info('{} 파일이 존재합니다.'.format(TOT_FILE_NM))
                    row = [PRVDR_CD,CAT_CD,TOT_FILE_NM]
                    FILE_DF = FILE_DF.append(pd.Series(row,index=FILE_DF.columns),ignore_index=True)
                    suc_cnt+=1
            else :
                err_msg = '해당되는 연계데이터 내역 (TBPINB103)이 없습니다.'
                raise custom_checkError(err_msg)

            msg = '검색된 파일 수 : {}개 / 검색 실패 파일수 : {}개'.format(suc_cnt,fail_cnt)
            logging.info(msg)
            print(msg)

            #하나이상 비식별파일이 없다면 배치실패 등록 및 다음 신청건수로 넘어감
            if fail_cnt != 0 :
                err_msg = '비식별 파일이 없는 개수 = {}개'.format(fail_cnt)
                raise custom_checkError(err_msg)

            #모든파일이 비식별파일로 있을경우 저장했던 FILE_DF로 다루기
            for idx in FILE_DF.index:
                PRVDR_CD = FILE_DF._get_value(idx,'PRVDR_CD')
                CAT_CD =  FILE_DF._get_value(idx,'CAT_CD')
                FILE_NM = FILE_DF._get_value(idx,'FILE_NM')


                #비식별 파일경로
                File_path = bobig_deid_in_data + '/' + FILE_NM
                #비식별 파일 복사경로
                copy_path = bobig_deid_in_data + '/work/' + FILE_NM
                #T_BDREQUESTINFO 의 CNORGCODE로 폐쇄전송 경로 설정
                bobig_move_path = RSLT_PTH_NM +'/'+ FILE_NM


                try:
                    os.mkdir(bobig_deid_in_data + '/work')
                except:
                    pass

                #먼저 파일을 복사
                logging.info('파일경로 :{} =>  복사경로 {}'.format(File_path, copy_path))
                print('파일경로 :{} =>  복사경로 {}'.format(File_path, copy_path))
                shutil.copy(File_path, copy_path)

                #복사경로에서 폐쇄망경로로 이동
                logging.info('파일경로 :{} =>  전송경로 {}'.format(copy_path,bobig_move_path))
                print('파일경로 :{} =>  전송경로 {}'.format(copy_path,bobig_move_path))
                shutil.move(copy_path, bobig_move_path)
                msg = '파일 : {} 폐쇄망전송 완료'.format(FILE_NM)
                logging.info(msg)
                print(msg)
                time.sleep(1)

                TBPBTV003_ins('002',msg)
                TBPINV114_ins_01(ask_id,rshp_id,PRVDR_CD,CAT_CD,exec_seq,FILE_NM)


                #성공한 경우 114에 기록
                logging.info('{} 전송완료'.format(FILE_NM))
                print('{} 전송완료'.format(FILE_NM))
            TBPBTV003_sel()
        except custom_checkError as e:
            err_msg = e.msg
            TBPBTV003_ins('902',err_msg)
            TBPBTV002_upt('902', '파이썬 작업 실패', err_msg)
            logging.info(err_msg)
            print(err_msg)
        except:
            err_msg = '  에러 == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
            TBPBTV002_upt('902','파이썬 작업 실패',err_msg)
            err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
            print(err_line,  err_msg)

    else :
        logging.info('상태코드가 {}인 배치현황이 없음 '.format(WK_DTL_TP_CD))


base_file_nm = os.path.basename(__file__).split('.')
logging.basicConfig(
    filename=LOG_DIR + '/' + base_file_nm[0] + '_' + datetime.now().strftime('%Y%m%d') + '.log', \
    level=eval(LOG_LEVEL), filemode='w', \
    format='%(levelname)s : line = %(lineno)d , message = %(message)s')

logging.info('IndividualDscr program start time == ' +datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

#전역변수 설정
conn = tibero_db_conn()
cur = conn.cursor()

WK_DTL_TP_CD = '85'
BT_SEQ = 0
BT_EXEC_SEQ = 0

if __name__ == "__main__":
    BT_SEQ = sys.argv[1]
    main()




cur.close()
conn.close()


