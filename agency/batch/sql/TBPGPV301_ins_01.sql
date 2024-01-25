INSERT INTO TBPGPV301 
(
	 OID
	,ASK_ID
	,RSHP_ID
	,WK_TP_CD_NM
	,WKUSR_ID
	,WKUSR_NM
	,PRCS_DT
	,PRCSTS
	,PRCS_CNTS
	,DTL_CNTS
)

VALUES 
(
	{oid}
	,'{ask_id}'
	,'{rshp_id}'
	,'{wk_tp_cd_nm}'
	,'{accessname}'
	,'임시'
	,SYSDATE
	,{prcsts}
	,'{result}'
	,'{content}'
)