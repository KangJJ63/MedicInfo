INSERT INTO TBPDIS{years}
(
 ASK_ID
,RSHP_ID
,PRVDR_CD
,CAT_CD
,EXEC_SEQ
,PRSNT_PG_NUM
,TOT_PG_NUM
,ORG_MRG_TP_CD
,DATUM_SEQ
,VAR_DSCR_SEQ
,STR_DATATP_VAL
,NUM_DATATP_VAL
,CRT_PGM_ID
,CRT_DT
)
VALUES
(
 '{ask_id}'
,'{rshp_id}'
,'{prvdr_cd}'
,'{cat_cd}'
,{exec_seq}
,{prsnt_pg_num}
,{tot_pg_num}
,'2'
,?
,?
,?
,?
,'{crt_pgm_id}'
,sysdate
)