SELECT 
     a.ASK_ID
    ,a.RSHP_ID
    ,a.PRVDR_CD
    ,a.CAT_CD
    ,LISTAGG(NVL(b.VAR_CD,a.VAR_CD), ',') WITHIN GROUP (ORDER BY a.VAR_DSCR_SEQ) as VAR_CD
    ,LISTAGG(a.VAR_DSCR_SEQ, ',') WITHIN GROUP (ORDER BY a.VAR_DSCR_SEQ) as VAR_DSCR_SEQ	
    --,substr(xmlagg(xmlelement(x,',',NVL(b.VAR_CD,a.VAR_CD)) order by a.VAR_DSCR_SEQ).extract('//text()'),2) as VAR_CD
    --,substr(xmlagg(xmlelement(x,',',a.VAR_DSCR_SEQ) order by a.VAR_DSCR_SEQ).extract('//text()'),2) as VAR_DSCR_SEQ
FROM TBPINM102 A
     LEFT JOIN TBPINM002 B ON (A.VAR_SEQ = B.VAR_SEQ) 
WHERE 1=1
AND A.ASK_ID = '{ask_id}'
AND A.RSHP_ID = '{rshp_id}'
AND A.PRVDR_CD = '{prvdr_cd}'
AND A.CAT_CD = '{cat_cd}'

group by 
     a.ASK_ID
    ,a.RSHP_ID
    ,a.PRVDR_CD
    ,a.CAT_CD
;