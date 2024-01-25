import os
import platform

# 서버환경
SERVER_ENV='INDEV'

# 파이썬폴더 기본 경로
if platform.system() == "Windows":
    BASE_DIR = 'C:/Users/lime-PC/Desktop/python_file/dev_ttp_fix/server/agency/batch'
elif platform.system() == "Linux":
    BASE_DIR = '/home/batch'

SQL_DIR= BASE_DIR + '/sql'
ORACLE_JDBC_DRIVE= 'oracle.jdbc.driver.OracleDriver'
ORACLE_DB_JAR= BASE_DIR + '/base/ojdbc6.jar'

LOG_DIR = BASE_DIR + '/logs'

##log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)##
LOG_LEVEL = 'logging.INFO'

####오라클 정보 입력
if SERVER_ENV == 'INDEV':
    ORACLE_DB_IP= '172.16.11.152'
    ORACLE_DB_PORT= '1521'
    ORACLE_DB_SID= 'ORCL'
    ORACLE_DB_USER= 'hmbp_orgdb'
    ORACLE_DB_PWD= '1234'
    # ORACLE_DB_USER= 'hmbpuser'
    # ORACLE_DB_PWD= 'hmbpuser'
elif SERVER_ENV == 'TEST':
    ORACLE_DB_IP= '172.16.11.152'
    ORACLE_DB_PORT= '1521'
    ORACLE_DB_SID= 'ORCL'
    ORACLE_DB_USER= 'hmbpuser'
    ORACLE_DB_PWD= 'hmbpuser'
elif SERVER_ENV == 'PROD':
    pass
else:
    pass

################# 기관 ################################
#개인식별정보 in 데이터 경로
def Agency_hash_in_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            HASH_IN_DIR = BASE_DIR + '/data/HASH_IN/1'
        elif prvdr_cd == 'K0002':
            HASH_IN_DIR = BASE_DIR + '/data/HASH_IN/2'
        elif prvdr_cd == 'K0003':
            HASH_IN_DIR = BASE_DIR + '/data/HASH_IN/3'
        elif prvdr_cd == 'K0004':
            HASH_IN_DIR = BASE_DIR + '/data/HASH_IN/4'
    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            HASH_IN_DIR = '/data/orgportal/FileStorage/IF_DL_301'
        elif prvdr_cd == 'K0002':
            HASH_IN_DIR = '/data/orgportal/FileStorage/IF_DL_302'
        elif prvdr_cd == 'K0003':
            HASH_IN_DIR = '/data/orgportal/FileStorage/IF_DL_303'
        elif prvdr_cd == 'K0004':
            HASH_IN_DIR = '/data/orgportal/FileStorage/IF_DL_304'
    return HASH_IN_DIR

#기관결합키 저장 경로
def Agency_hash_save_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            HASH_SAVE_DIR = BASE_DIR + '/data/IF_DL_300/1/work'
        elif prvdr_cd == 'K0002':
            HASH_SAVE_DIR = BASE_DIR + '/data/IF_DL_300/2/work'
        elif prvdr_cd == 'K0003':
            HASH_SAVE_DIR = BASE_DIR + '/data/IF_DL_300/3/work'
        elif prvdr_cd == 'K0004':
            HASH_SAVE_DIR = BASE_DIR + '/data/IF_DL_300/4/work'
    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            HASH_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_301/work'
        elif prvdr_cd == 'K0002':
            HASH_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_302/work'
        elif prvdr_cd == 'K0003':
            HASH_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_303/work'
        elif prvdr_cd == 'K0004':
            HASH_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_304/work'

    return HASH_SAVE_DIR

#기관결합키 전송 경로
def Agency_hash_out_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            HASH_OUT_DIR = BASE_DIR + '/data/IF_DL_300/1/send'
        elif prvdr_cd == 'K0002':
            HASH_OUT_DIR = BASE_DIR + '/data/IF_DL_300/2/send'
        elif prvdr_cd == 'K0003':
            HASH_OUT_DIR = BASE_DIR + '/data/IF_DL_300/3/send'
        elif prvdr_cd == 'K0004':
            HASH_OUT_DIR = BASE_DIR + '/data/IF_DL_300/4/send'

    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            HASH_OUT_DIR = '/nhis/file/data/indigo/IF_DL_301/send'
        elif prvdr_cd == 'K0002':
            HASH_OUT_DIR = '/data/indigo/IF_DL_302/send'
        elif prvdr_cd == 'K0003':
            HASH_OUT_DIR = '/data/indigo/IF_DL_303/send'
        elif prvdr_cd == 'K0004':
            HASH_OUT_DIR = '/data/indigo/IF_DL_304/send'

    return HASH_OUT_DIR

#기관 연계데이터 in 경로
def Agency_prov_in_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            PROV_IN_DIR = BASE_DIR + '/data/PROV_IN/1'
        elif prvdr_cd == 'K0002':
            PROV_IN_DIR = BASE_DIR + '/data/PROV_IN/2'
        elif prvdr_cd == 'K0003':
            PROV_IN_DIR = BASE_DIR + '/data/PROV_IN/3'
        elif prvdr_cd == 'K0004':
            PROV_IN_DIR = BASE_DIR + '/data/PROV_IN/4'

    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            PROV_IN_DIR = '/data/orgportal/FileStorage/IF_DL_501'
        elif prvdr_cd == 'K0002':
            PROV_IN_DIR = '/data/orgportal/FileStorage/IF_DL_502'
        elif prvdr_cd == 'K0003':
            PROV_IN_DIR = '/data/orgportal/FileStorage/IF_DL_503'
        elif prvdr_cd == 'K0004':
            PROV_IN_DIR = '/data/orgportal/FileStorage/IF_DL_504'
    return PROV_IN_DIR

#기관 연계데이터 저장 경로
def Agency_prov_save_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            PROV_SAVE_DIR = BASE_DIR + '/data/IF_DL_500/1/work'
        elif prvdr_cd == 'K0002':
            PROV_SAVE_DIR = BASE_DIR + '/data/IF_DL_500/2/work'
        elif prvdr_cd == 'K0003':
            PROV_SAVE_DIR = BASE_DIR + '/data/IF_DL_500/3/work'
        elif prvdr_cd:
            PROV_SAVE_DIR = BASE_DIR + '/data/IF_DL_500/4/work'

    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            PROV_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_501/work'
        elif prvdr_cd == 'K0002':
            PROV_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_502/work'
        elif prvdr_cd == 'K0003':
            PROV_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_503/work'
        elif prvdr_cd == 'K0004':
            PROV_SAVE_DIR = '/data/orgportal/FileStorage/IF_DL_504/work'
    return PROV_SAVE_DIR

#기관 연계데이터 전송 경로
def Agency_prov_out_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            PROV_OUT_DIR = BASE_DIR + '/data/IF_DL_500/1/send'
        elif prvdr_cd == 'K0002':
            PROV_OUT_DIR = BASE_DIR + '/data/IF_DL_500/2/send'
        elif prvdr_cd == 'K0003':
            PROV_OUT_DIR = BASE_DIR + '/data/IF_DL_500/3/send'
        elif prvdr_cd == 'K0004':
            PROV_OUT_DIR = BASE_DIR + '/data/IF_DL_500/4/send'

    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            PROV_OUT_DIR = '/nhis/file/data/indigo/IF_DL_501/send'
        elif prvdr_cd == 'K0002':
            PROV_OUT_DIR = '/data/indigo/IF_DL_502/send'
        elif prvdr_cd == 'K0003':
            PROV_OUT_DIR = '/data/indigo/IF_DL_503/send'
        elif prvdr_cd == 'K0004':
            PROV_OUT_DIR = '/data/indigo/IF_DL_504/send'
    return PROV_OUT_DIR

#메타파일 경로
def Agency_File_metaout_path(prvdr_cd):
    if platform.system() == "Windows":
        if prvdr_cd == 'K0001':
            META_OUT_DIR = BASE_DIR + '/data/Metafile/'
        elif prvdr_cd == 'K0002':
            META_OUT_DIR = BASE_DIR + '/data/Metafile/'
        elif prvdr_cd == 'K0003':
            META_OUT_DIR = BASE_DIR + '/data/Metafile/'
        elif prvdr_cd == 'K0004':
            META_OUT_DIR = BASE_DIR + '/data/Metafile/'

    elif platform.system() == "Linux":
        if prvdr_cd == 'K0001':
            META_OUT_DIR = '/nhis/file/data/indigo/IF_DL_D01/send'
        elif prvdr_cd == 'K0002':
            META_OUT_DIR = '/data/indigo/IF_DL_D02/send'
        elif prvdr_cd == 'K0003':
            META_OUT_DIR = '/data/indigo/IF_DL_D03/send'
        elif prvdr_cd == 'K0004':
            META_OUT_DIR = '/data/indigo/IF_DL_D04/send'
    return META_OUT_DIR


