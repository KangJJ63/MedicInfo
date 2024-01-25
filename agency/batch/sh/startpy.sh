#!/bin/bash 

# H1 기관해시내역 파일(Python)  
# H2 기관해시내역 파일 Move(Python)

YYYYMM=`date '+%Y%m'`
YYYYMMDD=`date '+%Y%m%d'`
PYTHON_CMD="python3.6"
PYTHON_OPTS=$1
PYTHON_ARG1=$2
PYTHON_HOME="/home/indigo/batch"
PYTHON_DIR=${PYTHON_HOME}"/bin"
LOG_DIR=${PYTHON_HOME}"/logs"

pid=`ps -ef | grep ${PYTHON_OPTS} | grep -v 'grep' |  awk '{print $2}'`
if [ "$pid"=="" ]
then
  cd ${LOG_DIR}
  tar -zcvf ${PYTHON_OPTS}${YYYYMM}.tar.gz ${PYTHON_OPTS}${YYYYMM}*.log
  cd ${PYTHON_DIR}
  nohup ${PYTHON_CMD} ${PYTHON_DIR}"/"${PYTHON_OPTS}".py" ${PYTHON_ARG1} >> ${LOG_DIR}"/"${PYTHON_OPTS}${YYYYMMDD}".log" 2>&1 &
else
  echo ${PYTHON_OPTS} "이 실행중이므로 작업을 실행하지 않았습니다." >> ${LOG_DIR}"/"${PYTHON_OPTS}${YYYYMMDD}".log" 2>&1
fi

