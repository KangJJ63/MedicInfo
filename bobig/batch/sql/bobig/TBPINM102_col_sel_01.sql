SELECT B.VAR_CD
FROM TBPINM102 A
			LEFT JOIN (SELECT VAR_SEQ,VAR_CD
								FROM TBPINM002
								WHERE 1=1
								AND PRVDR_CD = '{prvdr_cd}'
								AND CAT_CD = '{cat_cd}') B
								ON (A.VAR_SEQ = B.VAR_SEQ)
WHERE 1=1
AND A.ASK_ID = '{ask_id}'
AND A.RSHP_ID = '{rshp_id}'
AND A.PRVDR_CD = '{prvdr_cd}'
AND A.CAT_CD = '{cat_cd}'
AND A.ORG_MRG_TP_CD = '1'
ORDER BY A.VAR_DSCR_SEQ;