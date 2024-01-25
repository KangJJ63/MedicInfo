from base.base_bobig import *
import jaydebeapi

# 서버환경
SERVER_ENV='PROD_IN'

if SERVER_ENV == 'INDEV':
    ########## 개발 tibero DB ##########
    TIBERO_DB_IP2 = '172.16.11.152'
    TIBERO_DB_PORT2 = '8629'
    TIBERO_DB_SID2 = 'hmbp_dev'
    TIBERO_DB_USER2 = 'hmbpdata'
    TIBERO_DB_PWD2 = '1234'

elif SERVER_ENV == 'TEST':
    ##########행망 tibero DB ##########
    TIBERO_DB_IP2 = '10.182.107.173'
    TIBERO_DB_PORT2 = '8629'
    TIBERO_DB_SID2 = 'hmbp_testdb'
    TIBERO_DB_USER2 = 'hmbpuser'
    TIBERO_DB_PWD2 = 'qhqlr123$'

elif SERVER_ENV == 'PROD_IN':
    ##########행망 tibero DB ##########
    TIBERO_DB_IP2 = '10.182.107.171'
    TIBERO_DB_PORT2 = '8629'
    TIBERO_DB_SID2 = 'hmbp_indb'
    TIBERO_DB_USER2 = 'hmbpdata'
    TIBERO_DB_PWD2 = 'qhqlr$321'
else:
    pass

def tibero_db_conn2():

    conn = jaydebeapi.connect(
        TIBERO_JDBC_DRIVE,
        "jdbc:tibero:thin:@%s:%s:%s" % (TIBERO_DB_IP2 , TIBERO_DB_PORT2 , TIBERO_DB_SID2),
        [TIBERO_DB_USER2 , TIBERO_DB_PWD2],
        TIBERO_DB_JAR,
    )
    return conn

if __name__ == '__main__':
    print(tibero_db_conn2())


