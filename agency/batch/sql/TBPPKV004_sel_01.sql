SELECT A.FAIL_CNT,
       B.CNT
FROM   (SELECT COUNT(*) AS FAIL_CNT
        FROM TBPPKV004
        WHERE ASK_ID = '{ask_id}' AND
			  RSHP_ID = '{rshp_id}' AND
              TRNSMRCV_CD != 'S') A,
    (SELECT COUNT(*) AS CNT
    FROM TBPPKV004 A, TBPPKV003 B
    WHERE A.ASK_ID = '{ask_id}' AND
		  A.RSHP_ID = '{rshp_id}' AND
		  A.ASK_ID = B.ASK_ID AND
		  A.RSHP_ID = B.RSHP_ID AND
		  A.PRVDR_CD = B.PRVDR_CD AND
		  A.HASH_DID = B.HASH_DID
		  ) B