'''
1. 파일이름 : Summary02
2. 기능 : 비식별데이터 현황 작업 배치를 조회
3. 처리절차 : 기관데이터 배치 조회
    1) 배치 상태코드 84 조회
    2) 현황 로직 Sumamry_Functions로 현황 등록
    3) 작업결과저장
4. 최종수정 : 강재전 2020.12.18
5. 버전
    0.95 2020-10-5 통합테스트완료
    1.0  2020.10.5 배포
    1.01 2020.10.6 소스디렉토리 조정
'''


import os, sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))
from bin.Summary_Functions import *
from base.tibero_dbconn import *
base_file_nm = os.path.basename(__file__).split('.')
file_pre_name = base_file_nm[0]

#비식별데이터 현황 상태 코드
WK_DTL_TP_CD = '84'

def main(string):
    Summary_Logic(file_pre_name, WK_DTL_TP_CD,string)

if __name__ == "__main__" :
    main(sys.argv[1])

