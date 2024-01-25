MERGE INTO TBPINV105
			 USING  DUAL
			  		ON (ASK_ID = '{ask_id}' AND
			  				 RSHP_ID = '{rshp_id}' AND
			  				 PRVDR_CD = '{prvdr_cd}' AND
			  				 CAT_CD = '{cat_cd}' AND
			  				 ORG_MRG_TP_CD = '{org_mrg_tp_cd}' AND
			  				 PRSNT_PG_NUM = {prsnt_pg_nm} AND
			  				 TOT_PG_NUM = {tot_pg_nm})
			  				 
WHEN MATCHED THEN
			  UPDATE SET CRT_YMD ='{local_file_ymd}',
			   			 CRT_DT = SYSDATE
WHEN NOT MATCHED THEN
			  INSERT ( 
								     ASK_ID
									,RSHP_ID
									,PRVDR_CD
									,CAT_CD
									,ORG_MRG_TP_CD
									,PRSNT_PG_NUM
									,TOT_PG_NUM
									,CRT_YMD
									,FILE_SZ
									,DATUM_CNT
									,VAR_EA
									,FILE_NM
									,CRT_PGM_ID
									,CRT_DT)
				VALUES  (
										 '{ask_id}'
										,'{rshp_id}'
										,'{prvdr_cd}'
										,'{cat_cd}'
										,'{org_mrg_tp_cd}'
										,{prsnt_pg_nm}
										,{tot_pg_nm}
										,'{local_file_ymd}'
										,{file_size}
										,{file_rows_size}
										,{file_columns_size}
										,'{file_nm}'
										,'{crt_pgm_id}'
										,SYSDATE
			   						)