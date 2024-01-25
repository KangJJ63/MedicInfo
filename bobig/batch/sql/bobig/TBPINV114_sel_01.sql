SELECT DISTINCT 
					A.PRVDR_CD
				   ,A.CAT_CD
				   ,B.WK_EXEC_CNTS
				   ,A.DI_WK_DIV_CNTS
FROM	(SELECT ASK_ID
			   ,RSHP_ID
			   ,PRVDR_CD
			   ,CAT_CD
			   ,EXEC_SEQ
			   ,WK_EXEC_CNTS
			   ,CRT_DT
			   ,DI_WK_DIV_CNTS
		 FROM TBPINV114
		 WHERE DI_WK_DIV_CNTS = '{di_wk_div_cnts}'
		 ORDER BY CRT_DT DESC) A,
					
		(SELECT A.ASK_ID
			   ,A.RSHP_ID
			   ,A.EXEC_SEQ
			   ,B.WK_EXEC_CNTS
		 FROM TBPBTV001 A
			 ,TBPBTV003 B
		 WHERE A.WK_DTL_TP_CD = '{di_wk_div_cnts}' AND
               A.EXEC_SEQ = {exec_seq} AND  
               A.BT_SEQ = B.BT_SEQ AND
               B.WK_EXEC_STS_CD IN (002,003) )B
					                
WHERE A.ASK_ID = B.ASK_ID AND
 	  A.RSHP_ID = B.RSHP_ID AND
 	  A.EXEC_SEQ = B.EXEC_SEQ AND
 	  A.WK_EXEC_CNTS = B.WK_EXEC_CNTS AND
 	  A.ASK_ID = '{ask_id}';				