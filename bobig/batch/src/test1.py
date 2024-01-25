

def File_Naming_Check(ASK_ID,RSHP_ID,PRVDR_CD,CAT_CD,FILE_NM):

    File_Split_Nm = FILE_NM.split('_')
    print(File_Split_Nm)
    file_prvdr_num = File_Split_Nm[2]
    file_ask_id = File_Split_Nm[3][:4] + '-' + File_Split_Nm[3][4:]
    file_rshp_id = File_Split_Nm[4]
    if len(File_Split_Nm[5:-3]) > 1 :
        file_cat_cd = '_'.join(File_Split_Nm[5:-3])
    else:
        file_cat_cd = File_Split_Nm[5]
    PR_PG_NUM = File_Split_Nm[-3]
    TOT_PG_NUM = File_Split_Nm[-2]

    if PRVDR_CD == 'K0001':
        match_prvdr_num = '501'
    elif PRVDR_CD == 'K0002':
        match_prvdr_num = '502'
    elif PRVDR_CD == 'K0003':
        match_prvdr_num = '503'
    else :
        match_prvdr_num = '504'

    print('''### File_Naming_Check ###
    file_ask_id = {} , file_rshp_id = {}, file_prvdr_num = {}, file_cat_cd = {}
        PR_PG_NUM = {}, PR_PG_NUM = {}'''
        .format(file_ask_id,file_rshp_id,file_prvdr_num,file_cat_cd,PR_PG_NUM,TOT_PG_NUM))
    print('''### File_Naming_Check ###
    ASK_ID = {} , RSHP_ID = {}, match_prvdr_num = {}, CAT_CD = {}'''
        .format(ASK_ID,RSHP_ID,match_prvdr_num,CAT_CD))

    return file_ask_id == ASK_ID and file_prvdr_num == match_prvdr_num and file_rshp_id == RSHP_ID and PR_PG_NUM <= TOT_PG_NUM and file_cat_cd == CAT_CD

File_Naming_Check('2019-00082','A0001','K0002','TWJHC200','IF_DL_502_201900082_A0001_TWJHC200_89_144_20210226.txt')

