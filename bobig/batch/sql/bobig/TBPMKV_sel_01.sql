SELECT distinct
HASH_DID
,ALTER_ID
FROM TBPMKV{ask_id_date}
WHERE ASK_ID = '{ask_id}'
AND RSHP_ID  = '{rshp_id}'
