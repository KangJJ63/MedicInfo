UPDATE TBPBTV002 
SET  
    WK_END_DT = SYSDATE, 
    WK_STS_CD = '{wk_sts_cd}',
    WK_RSLT_CNTS = '{wk_rslt_cnts}',
    WK_RSLT_DTL_CNTS = '{wk_rslt_dtl_cnts}',
    CRT_DT = SYSDATE
WHERE BT_EXEC_SEQ = {bt_exec_seq} and
      BT_SEQ = {bt_seq}