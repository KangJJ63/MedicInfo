SELECT PRVDR_CD,
	   CAT_CD,
	   FILE_NM,
	   PRCS_FAIL_YN
FROM TBPINB103
WHERE 1=1 
	  AND ASK_ID = '{ask_id}' 
	  AND RSHP_ID = '{rshp_id}' 
;