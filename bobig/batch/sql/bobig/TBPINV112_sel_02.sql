SELECT A.VAR_CD,B.DI_SET_VAL
FROM (
		SELECT A.ASK_ID,
						A.RSHP_ID,
						A.PRVDR_CD,
						A.CAT_CD,
						B.VAR_CD,
						A.VAR_DSCR_SEQ
		FROM  TBPINM102 A
						LEFT JOIN (SELECT ASK_ID,RSHP_ID,PRVDR_CD,CAT_CD,VAR_SEQ,VAR_CD
											 FROM TBPINM102
											WHERE 1=1
											AND ORG_MRG_TP_CD = '2') B
								  ON  (A.ASK_ID = B.ASK_ID
											AND A.RSHP_ID = B.RSHP_ID
											AND A.PRVDR_CD = B.PRVDR_CD
											AND A.CAT_CD = B.CAT_CD
											)		
		WHERE 1=1
		AND A.VAR_SEQ = B.VAR_SEQ
		AND A.ORG_MRG_TP_CD = '1'
		AND A.ASK_ID = '{ask_id}'
		AND A.RSHP_ID = '{rshp_id}'
		AND A.PRVDR_CD = '{prvdr_cd}'
		AND A.CAT_CD = '{cat_cd}'
		) A , TBPINV112 B
WHERE 1=1
AND A.ASK_ID = B.ASK_ID
AND A.RSHP_ID = B.RSHP_ID
AND A.PRVDR_CD = B.PRVDR_CD
AND A.CAT_CD = B.CAT_CD
AND A.VAR_DSCR_SEQ = B.VAR_DSCR_SEQ
AND B.DI_SET_VAL IS NOT NULL
AND B.EXEC_SEQ = {exec_seq}