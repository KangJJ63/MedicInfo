 INSERT INTO TBIPKV{year} NOLOGGING
(ASK_ID
, RSHP_ID
, PRVDR_CD 
, HASH_DID
, TRNSMRCV_CRT_DT
, TRNSMRCV_FIN_DT
, TRNSMRCV_CD
, CRT_PGM_ID
, CRT_DT)
 VALUES
( '{ask_id}'
, '{rshp_id}'
, '{prvdr_cd}'
, ?
, SYSDATE
, SYSDATE
, 'S'
, '{crt_pgm_id}'
, SYSDATE ) ;
