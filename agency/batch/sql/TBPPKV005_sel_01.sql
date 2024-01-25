select 
 ASK_ID
,RSHP_ID
,PRVDR_CD
,HASH_DID
,MRG_NM
,MRG_SSN
from TBPPKV005
where 1=1
and ASK_ID = '{ask_id}'
and RSHP_ID = '{rshp_id}'
and PRVDR_CD = '{prvdr_cd}'
;