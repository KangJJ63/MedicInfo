SELECT 
	D.PRVDR_CD
	,D.CAT_CD
FROM T_BDREQUESTINFO A 
	 JOIN TBPBTV001 C ON (A.REQUESTID = C.ASK_ID
	                  AND A.HYPOID = C.RSHP_ID)
 	 JOIN TBPINV115 D ON ( C.ASK_ID = D.ASK_ID
		 			  AND C.RSHP_ID = D.RSHP_ID )
WHERE 1=1 
AND C.ASK_ID = '{ask_id}'
AND C.RSHP_ID = '{rshp_id}'
AND C.BT_SEQ = {bt_seq}
AND C.WK_DTL_TP_CD = '{wk_dtl_tp_cd}'
AND D.EXEC_SEQ = {exec_seq} ;
