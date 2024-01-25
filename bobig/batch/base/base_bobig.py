import os
import platform

# 서버환경
SERVER_ENV='PROD_OUT'

# 파이썬폴더 경로
if platform.system() == "Windows":
    BASE_DIR = 'd:/work/workspace/bobig/batch'
    # BASE_DIR = 'C:/Users/lime-PC/Desktop/python_file/dev_ttp_fix/server/Integrated_batch/batch'
elif platform.system() == "Linux":
    BASE_DIR = '/home/batch'

SQL_DIR= BASE_DIR + '/sql/bobig'
LOG_DIR = BASE_DIR + '/logs'
LOG_LEVEL = 'logging.INFO'
TIBERO_JDBC_DRIVE= 'com.tmax.tibero.jdbc.TbDriver'
TIBERO_DB_JAR= BASE_DIR + '/base/tibero6-jdbc.jar'



if SERVER_ENV == 'INDEV':
    ########## 개발 tibero DB ##########
    TIBERO_DB_IP= '172.16.11.152'
    TIBERO_DB_PORT= '8629'
    TIBERO_DB_SID= 'hmbp_dev'
    TIBERO_DB_USER= 'hmbpuser'
    TIBERO_DB_PWD= '1234'

elif SERVER_ENV == 'TEST':
    ##########행망 tibero DB ##########
    TIBERO_DB_IP = '10.182.107.173'
    TIBERO_DB_PORT = '8629'
    TIBERO_DB_SID = 'hmbp_testdb'
    TIBERO_DB_USER = 'hmbpuser'
    TIBERO_DB_PWD = 'qhqlr123$'

elif SERVER_ENV == 'PROD_OUT':
    ##########행망 tibero DB ##########
    TIBERO_DB_IP = '10.182.107.170'
    TIBERO_DB_PORT = '8629'
    TIBERO_DB_SID = 'hmbp_outdb'
    TIBERO_DB_USER = 'hmbpuser'
    TIBERO_DB_PWD = 'qhqlr$321'
    
elif SERVER_ENV == 'PROD_IN':
    ##########행망 tibero DB ##########
    TIBERO_DB_IP = '10.182.107.171'
    TIBERO_DB_PORT = '8629'
    TIBERO_DB_SID = 'hmbp_indb'
    TIBERO_DB_USER = 'hmbpdata'
    TIBERO_DB_PWD = 'qhqlr$321'
else:
    pass


#################################
#보빅 경로 함수
#################################


#기관별 기관데이터 경로 반환 함수
def Bobig_ori_prov_path():
    if platform.system() == 'Windows':
        path = BASE_DIR + '/data/500'
    elif platform.system() == 'Linux' :
        path = '/esb_nfs/esbmst/indigo/IF_DL_500/recv'
    return path

#기관별 비식별 파일 저장 경로
def Bobig_deid_path(prvdr):
    if platform.system() == 'Windows':
        if prvdr == 'K0001':
            path = BASE_DIR + '/data/801'
        else :
            path = BASE_DIR + '/data/802'

    elif platform.system() == 'Linux':
        if prvdr == 'K0001':
            path = '/esb_nfs/data/IF_DL_801'
        else:
            path = '/esb_nfs/data/IF_DL_802'
    # 원본요약 저장경로 생성 ( 이미 있으면 pass)
    try:
        os.makedirs(path)
    except:
        pass

    return path

#기관요약데이터 저장 경로 함수
def Bobig_sumjob01_path(prvdr):
    if platform.system() == 'Windows':
        if prvdr == 'K0001':
            path = BASE_DIR + '/data/summary/IF_DL_501'
        elif prvdr == 'K0002':
            path = BASE_DIR + '/data/summary/IF_DL_502'
        elif prvdr == 'K0003':
            path = BASE_DIR + '/data/summary/IF_DL_503'
        else:
            path = BASE_DIR + '/data/summary/IF_DL_504'
    elif platform.system() == 'Linux':
        if prvdr == 'K0001':
            path = '/esb_nfs/data/summary/IF_DL_501'
        elif prvdr == 'K0002':
            path = '/esb_nfs/data/summary/IF_DL_502'
        elif prvdr == 'K0003':
            path = '/esb_nfs/data/summary/IF_DL_503'
        else:
            path = '/esb_nfs/data/summary/IF_DL_504'

    # 원본요약 저장경로 생성 ( 이미 있으면 pass)
    try:
        os.makedirs(path)
    except:
        pass


    return path

#비식별요약데이터 저장 경로
def Bobig_sumjob02_path(prvdr):
    if platform.system() == 'Windows':
        if prvdr == 'K0001':
            path = BASE_DIR + '/data/summary/IF_DL_801'
        else :
            path = BASE_DIR + '/data/summary/IF_DL_802'

    elif platform.system() == 'Linux':
        if prvdr == 'K0001':
            path = '/esb_nfs/data/summary/IF_DL_801'
        else:
            path = '/esb_nfs/data/summary/IF_DL_802'

    # 원본요약 저장경로 생성 ( 이미 있으면 pass)
    try:
        os.makedirs(path)
    except:
        pass

    return path

#비식별 데이터 폐쇄망 전송 경로
def Bobig_move_deid_data_path(prvdr):
    if platform.system() == 'Windows':
        if prvdr == 'K0001':
            path = BASE_DIR + '/data/move_out/1'
        else:
            path = BASE_DIR + '/data/move_out/2'
    elif platform.system() == 'Linux':
        if prvdr == 'K0001':
            path = '/esb_nfs/esbmst/indigo/IF_DL_801/send'
        else:
            path = '/esb_nfs/esbmst/indigo/IF_DL_802/send'

    return path


def set_zip_path(ifid):
    if platform.system() == 'Windows':
        return BASE_DIR + '/data/summary/'+ifid
    elif platform.system() == 'Linux':
        return '/esb_nfs/data/summary/'+ifid