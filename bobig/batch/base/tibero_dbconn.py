from base.base_bobig import *
import jaydebeapi

def tibero_db_conn():

    conn = jaydebeapi.connect(
        TIBERO_JDBC_DRIVE,
        "jdbc:tibero:thin:@%s:%s:%s" % (TIBERO_DB_IP , TIBERO_DB_PORT , TIBERO_DB_SID),
        [TIBERO_DB_USER , TIBERO_DB_PWD],
        TIBERO_DB_JAR,
    )
    return conn

if __name__ == '__main__':
    print(tibero_db_conn())


