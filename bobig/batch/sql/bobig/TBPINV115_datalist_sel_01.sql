SELECT PRVDR_CD
 				,LISTAGG(CAT_CD, ',') WITHIN GROUP(ORDER BY CAT_CD) AS CAT_CD
FROM TBPINV115
WHERE 1=1
AND ASK_ID = '{ask_id}'
AND RSHP_ID = '{rshp_id}' 
AND EXEC_SEQ = {exec_seq}
GROUP BY PRVDR_CD;