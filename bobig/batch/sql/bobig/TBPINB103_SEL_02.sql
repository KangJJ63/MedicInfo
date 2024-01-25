SELECT FILE_NM,
	   PRCS_FAIL_YN
FROM TBPINB103
WHERE 
	  ASK_ID = '{ask_id}' AND
	  RSHP_ID = '{rshp_id}' AND
	  PRVDR_CD = '{prvdr_cd}' AND
	  CAT_CD = '{cat_cd}'
;