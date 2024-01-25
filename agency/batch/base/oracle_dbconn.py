from base.base_agency import *
import jaydebeapi

def oracle_db_conn():
    conn = jaydebeapi.connect(
        ORACLE_JDBC_DRIVE,
        "jdbc:oracle:thin:@%s:%s:%s" % (ORACLE_DB_IP , ORACLE_DB_PORT , ORACLE_DB_SID),
        [ORACLE_DB_USER , ORACLE_DB_PWD],
        ORACLE_DB_JAR,
    )
    return conn

if __name__ == '__main__':
    print("asdfasdfasdfasdf")
