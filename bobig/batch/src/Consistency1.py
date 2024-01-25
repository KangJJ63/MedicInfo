'''
1. 파일이름 : Consistency
2. 기능 : 기관데이터 정합성 검증 성공/실패를 판단한다.
3. 처리절차 : 정합성 검증
    1) TBPGPM107 신청변수 조회
    2) 기관데이터 컬럼명과 TBPGPM107 변수 비교
    3) 작업결과저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.12.19 배포
    1.01 2020.12.19 소스디렉토리 조정
'''
import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from base.tibero_dbconn import *
from base.query_sep import *
import logging,datetime


class null_checkError(Exception):
    def __init__(self, msg):
        self.msg = msg

    def __str__(self):
        return self.msg

'''
1. 기능 : 정합성 검증 비교
2. 최종수정자 : 강재전 2020.10.27
3. 파라미터 : 신청ID,가설ID,기관코드,데이터셋명
4. retrun : 정합성 검증 성공 유무
'''
def Consistency_check(cur,col,ASK_ID,RSHP_ID,PRVDR_CD,CAT_CD) :

        #data_sel_107_src = SQL_DIR + '/' + 'TBPGPM107_sel_01.sql'
        data_sel_107_src = SQL_DIR + '/' + 'TBPINM102_sel_01.sql'
        data_sel_107 = query_seperator(data_sel_107_src).format(ask_id = ASK_ID,
                                                            rshp_id = RSHP_ID,
                                                            prvdr_cd =PRVDR_CD,
                                                            cat_cd = CAT_CD)
        cur.execute(data_sel_107)
        data_sel_107_fetch = cur.fetchone()
        try:
            if data_sel_107_fetch is None:
                err_msg = '데이터셋 {}와 매치되는 TBPINM102 값이 없습니다.'.format(CAT_CD)
                raise null_checkError(err_msg)
            file_meta_col = ['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'HASH_DID','DATASET']
            # file_meta_col = ['ASK_ID', 'RSHP_ID', 'PRVDR_CD', 'HASH_DID','CAT_CD']

            db_col_meta = [x.upper() for x in data_sel_107_fetch[4].split(',')] + file_meta_col

            #컬럼 개수가 같은지 확인
            if len(col) == len(db_col_meta):
                #만약 같다면
                for word in db_col_meta:
                    if word not in col:
                        print('데이터 파일에 {}컬럼이 없습니다.'.format(word))
                        return False
                return True

            else:

                if len(col) > len(db_col_meta) :
                    fail_col = list(set(col).difference(db_col_meta))
                    err_msg = '컬럼수 불일치 데이터파일 컬럼수 ={}, TBPINM102 컬럼수 ={}\n 데이터파일 추가 컬럼 = {} '.format(len(col), len(db_col_meta),fail_col)
                else :
                    fail_col = list(set(db_col_meta).difference(col))
                    err_msg = '컬럼수 불일치 데이터파일 컬럼수 ={}, TBPINM102 컬럼수 ={}\n TBPINM102 추가 컬럼 = {} '.format(len(col),
                                                                                                    len(db_col_meta),
                                                                                                    fail_col)
                logging.error(err_msg)
                print(err_msg)
                return False

        except null_checkError as e:
            err_msg = e.msg
            print(err_msg)
            return False

        except:
            err_line = 'err_line == {}'.format(sys.exc_info()[-1].tb_lineno)
            err_msg = '  err_msg == ' + ' '.join(map(str, sys.exc_info()[:2])).replace('\'', '')
            print(err_msg,err_line)
            return False
