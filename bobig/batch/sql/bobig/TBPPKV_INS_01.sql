 INSERT INTO TBPPKV{year} NOLOGGING
(ASK_ID
, RSHP_ID
, PRVDR_CD 
, HASH_DID
, CRT_PGM_ID
, CRT_DT)
 VALUES
( '{ask_id}'
, '{rshp_id}'
, '{prvdr_cd}'
, ?
, '{crt_pgm_id}'
, SYSDATE ) ;
