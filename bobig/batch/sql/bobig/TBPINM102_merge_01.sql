MERGE INTO HMBPUSER.TBPINM102 AS  A
USING (
	WITH A AS (
				SELECT  
						ASK_ID
						,RSHP_ID
						,PRVDR_CD
						,ORG_MRG_TP_CD
						,CAT_CD
						,VAR_SEQ
						,REQ_VAR_TP_CD
						,VAR_DSCR_SEQ
						,FILE_NM
						,'G' || VAR_DSCR_SEQ AS VAR_DSCR_NM
						,VAR_CD
						,CRT_DT
				FROM (
						SELECT 
								 A.ASK_ID
								,A.RSHP_ID
								,A.PRVDR_CD
								,'2' AS ORG_MRG_TP_CD
								,A.CAT_CD
								,A.VAR_SEQ
								,A.REQ_VAR_TP_CD
								,RANK() OVER (ORDER BY A.VAR_SEQ) AS VAR_DSCR_SEQ
								,CASE 
								WHEN A.PRVDR_CD = 'K0001' THEN 'IF_DL_501'
								WHEN A.PRVDR_CD = 'K0002' THEN 'IF_DL_502'
								WHEN A.PRVDR_CD = 'K0003' THEN 'IF_DL_503'
								WHEN A.PRVDR_CD = 'K0004' THEN 'IF_DL_504'
								ELSE 'IF_DL_500' END FILE_NM
								,B.VAR_CD
								,SYSDATE AS CRT_DT
						FROM 
							 TBPINM102 A
							,TBPINM002 B 
							,TBPINM005 C
							,TBPINV112 D
						WHERE 1= 1
						AND A.ASK_ID = '{ask_id}'
						AND A.RSHP_ID = '{rshp_id}'
						AND A.PRVDR_CD = '{prvdr_cd}'
						AND A.CAT_CD = '{cat_cd}'
						AND A.ORG_MRG_TP_CD = '1' 
						AND A.VAR_SEQ = B.VAR_SEQ
						AND B.VAR_SEQ = C.VAR_SEQ
						AND A.VAR_SEQ = C.VAR_SEQ
						AND A.ASK_ID = D.ASK_ID
						AND A.RSHP_ID = D.RSHP_ID
						AND A.PRVDR_CD = D.PRVDR_CD
						AND A.CAT_CD = D.CAT_CD
						AND A.VAR_DSCR_SEQ = D.VAR_DSCR_SEQ
						AND D.DI_SET_VAL IS NOT NULL
					GROUP BY 
						 A.ASK_ID
						,A.RSHP_ID
						,A.CAT_CD
						,A.PRVDR_CD
						,A.VAR_DSCR_NM
						,B.VAR_CD
						,A.VAR_SEQ
						,A.VAR_DSCR_SEQ
						,A.REQ_VAR_TP_CD
				) TB2
			ORDER BY VAR_SEQ
					)
	SELECT 
			 ASK_ID
			,RSHP_ID
			,PRVDR_CD
			,ORG_MRG_TP_CD
			,CAT_CD
			,VAR_SEQ
			,REQ_VAR_TP_CD
			,VAR_DSCR_SEQ
			,FILE_NM
			,VAR_DSCR_NM
			,VAR_CD
			,'{crt_pgm_id}' CRT_PGM_ID
			,CRT_DT
	FROM A
	UNION ALL 
	SELECT 
		  A.ASK_ID
		,A.RSHP_ID
		,A.PRVDR_CD
		,A.ORG_MRG_TP_CD
		,A.CAT_CD
		,NULL
		,'2' 
		,(MAX(A.VAR_DSCR_SEQ) +1)
		,A.FILE_NM
		,CONCAT('G',MAX(A.VAR_DSCR_SEQ) +1)
		,'CNT'
		,'{crt_pgm_id}' CRT_PGM_ID
		,SYSDATE
	FROM A 
	GROUP BY 
		 A.ASK_ID
		,A.RSHP_ID
		,A.PRVDR_CD
		,A.ORG_MRG_TP_CD
		, A.CAT_CD
		,A.FILE_NM
	)  AS B
ON 
(
	 A.ASK_ID         = B.ASK_ID
	AND A.RSHP_ID     = B.RSHP_ID
	AND A.CAT_CD      = B.CAT_CD
	AND A.PRVDR_CD    = B.PRVDR_CD
	AND A.VAR_DSCR_SEQ = B.VAR_DSCR_SEQ
	AND A.ORG_MRG_TP_CD = B.ORG_MRG_TP_CD
)
WHEN MATCHED THEN 
UPDATE SET 
	 A.REQ_VAR_TP_CD = B.REQ_VAR_TP_CD
	,A.VAR_DSCR_NM = B.VAR_DSCR_NM
	,A.VAR_CD      = B.VAR_CD
	,A.VAR_SEQ     = B.VAR_SEQ
	,A.UPD_DT = SYSDATE
WHEN NOT MATCHED THEN 
INSERT (A.ASK_ID,A.RSHP_ID,A.CAT_CD,A.PRVDR_CD,A.VAR_DSCR_NM,A.VAR_CD,A.VAR_SEQ,A.ORG_MRG_TP_CD,A.VAR_DSCR_SEQ,A.REQ_VAR_TP_CD,A.CRT_PGM_ID,A.CRT_DT) 
VALUES (B.ASK_ID,B.RSHP_ID,B.CAT_CD,B.PRVDR_CD,B.VAR_DSCR_NM,B.VAR_CD,B.VAR_SEQ,B.ORG_MRG_TP_CD,B.VAR_DSCR_SEQ,B.REQ_VAR_TP_CD,B.CRT_PGM_ID,B.CRT_DT)