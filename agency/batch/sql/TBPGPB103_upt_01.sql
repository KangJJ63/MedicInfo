UPDATE TBPGPB103 SET PRCS_DT = SYSDATE, 
					 PRCS_FAIL_YN = '{prcs_fail_yn}' , 
					 PRCS_FAIL_CNTS = '{prcs_fail_cnts}'
WHERE ASK_ID = '{ask_id}' AND
      RSHP_ID = '{rshp_id}' AND
	  FILE_NM = '{file_nm}'
