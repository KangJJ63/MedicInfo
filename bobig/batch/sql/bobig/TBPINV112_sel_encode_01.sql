SELECT B.VAR_CD
	  ,A.DI_SET_VAL
FROM TBPINV112 A
		JOIN (SELECT ASK_ID
					,RSHP_ID
					,PRVDR_CD
					,CAT_CD
					,VAR_CD
					,VAR_DSCR_SEQ
			  FROM TBPINM102 
			  WHERE 1=1
				AND ORG_MRG_TP_CD = '1') B 
		  
		  ON (A.ASK_ID = B.ASK_ID
		      AND A.RSHP_ID = B.RSHP_ID
			  AND A.PRVDR_CD = B.PRVDR_CD
			  AND A.CAT_CD = B.CAT_CD
		      AND A.VAR_DSCR_SEQ = B.VAR_DSCR_SEQ)
			  
WHERE A.ASK_ID = '{ask_id}'
AND A.RSHP_ID = '{rshp_id}'
AND A.PRVDR_CD = '{prvdr_cd}'
AND A.CAT_CD = '{cat_cd}'
AND A.EXEC_SEQ = {exec_seq}
AND A.DI_SET_VAL IS NOT NULL
