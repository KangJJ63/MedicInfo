SELECT /*+ parallel(4) full(A) */
       HASH_DID
  FROM TBIPKV{year}  A
 WHERE 1 = 1 
   AND A.ASK_ID = '{ask_id}'
   AND A.RSHP_ID = '{rshp_id}'
   AND A.PRVDR_CD = '{prvdr_cd}'

