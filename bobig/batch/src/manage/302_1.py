from concurrent.futures import ProcessPoolExecutor
from functools import partial
import os, csv, platform
import pandas as pd

def modifyfile(config, tqdm=lambda x: x):
    '''
    Args:
        - config:  parameters
            - in_dir: input file directory
            - in_file : search for input file name
            - out_dir: output file directory
            - num_workers: Optional, number of worker process to parallelize across
            - tqdm: Optional, provides a nice progress bar

    Returns:
        - A list of tuple describing the train examples. this should be written to train.txt
    '''

    executor = ProcessPoolExecutor(max_workers=config.num_workers)
    futures = []
    index = 1

    fileList = [file for file in os.listdir(config.in_dir) if file.startswith(config.in_file)]

    for file in fileList:
        print("input file ={}".format(file))
        futures.append(executor.submit(partial(_process_utterance, 
            config.in_dir, config.out_dir, file, config.header_dir, config.header_file, config.cat_cd)))
        index += 1

    return [future.result() for future in tqdm(futures) if future.result() is not None]

def _process_utterance(in_dir, out_dir, out_file, header_dir, header_file, cat_cd):
    '''
        1. 심평원데이터 레코드마지막에 콤마가 있어 제거
        2. ASK_ID - 없음
        3. header 는 사용하지 않음
        df = pd.DataFrame({'x1':[1,2,3], 'x2':[11,22,33], 'x3':['111','222','333']})
        print(df.x3.str[0:1])

    '''
    # 고정되어 변경되지 않음 : 표준
    names = ['ASK_ID_NO_DASH','RSHP_ID','PRVDR_CD','HASH_DID', 'DUMMY']

    content = os.path.join(in_dir, out_file)
    content_df = pd.read_csv(content, names=names, header=None, dtype='str')
    ask_df = content_df
    # newRow.append(readRow[0][0:4]+'-'+readRow[0][4:9])    
    content_df.insert(0, 'ASK_ID', ask_df.ASK_ID_NO_DASH.str[0:4] + '-' + ask_df.ASK_ID_NO_DASH.str[4:9])
    content_df.drop(columns=['ASK_ID_NO_DASH', 'DUMMY'], inplace=True)

    #file_write = None
    if out_dir is not None:
        out = os.path.join(out_dir, out_file)
        content_df.to_csv(out, index=False)


# --name 302_1 --ask_id 2020-00023 --rshp_id A0001 --prvcd_cd K0002 
# --in_dir d:/data/in --in_file IF_DL_302_202000023_A0001
# --out_dir d:/data/out

# python3.6 src/managefile.py 
# --name 302_1 --ask_id 2020-00023 --rshp_id A0001 --prvcd_cd K0002 
# --in_dir /esb_nfs/esbmst/indigo/IF_DL_300/send/error --in_file IF_DL_302_202000023_A0001
# --out_dir /esb_nfs/2020/202000023/IF_DL_3
# def _process_utterance(in_dir, in_file, out_dir, out_file, header_dir, header_file, cat_cd):


if __name__ == "__main__":
    import argparse
    from tqdm import tqdm
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', type=str, default='502_3')
    parser.add_argument('--ask_id', type=str, default='2019-00082')
    parser.add_argument('--rshp_id', type=str, default='A0001')
    parser.add_argument('--prvcd_cd', type=str, default='K0002')
    parser.add_argument('--cat_cd', type=str, default='TWJHC200')
    parser.add_argument('--num_workers', type=int, default=None)
    parser.add_argument('--header_dir', type=str, default='d:/data')
    parser.add_argument('--header_file', type=str, default='IF_DL_502_201900082_A0001_TWJHC200.txt')
    parser.add_argument('--in_dir', type=str, default='d:/data/in')
    parser.add_argument('--in_file', type=str, default='IF_DL_502_201900082_A0001_TWJHC200')
    parser.add_argument('--out_dir', type=str, default='d:/data/out')
    #parser.add_argument('--out_file', type=str, default=None)
    config = parser.parse_args()
    # modifyfile(config, tqdm=tqdm)  

    fileList = [file for file in os.listdir(config.in_dir) if file.startswith(config.in_file)]
    for file in fileList:
        _process_utterance(config.in_dir, config.out_dir, file, 
            config.header_dir, config.header_file, config.cat_cd)



