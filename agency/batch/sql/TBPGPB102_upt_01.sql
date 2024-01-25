UPDATE TBPGPB102 B 
SET B.PRCS_DT = SYSDATE, 
	B.PRCS_FAIL_YN = '{prcs_fail_yn}' , 
	B.PRCS_FAIL_CNTS = '{prcs_fail_cnts}'
WHERE 
EXISTS (
		SELECT 1
		FROM TBPGPM101 A
		WHERE 1=1
		AND A.OID = B.PRNT_OID
		AND A.OID = '{oid}'
		AND B.FILE_NM = '{file_nm}'
		);
