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
    '''

    content = os.path.join(in_dir, out_file)
    content_df = pd.read_csv(content, dtype='str')
    content_df.drop(columns=['Unnamed: ' + str(len(content_df.columns)-1)], inplace=True)

    #file_write = None
    if out_dir is not None:
        out = os.path.join(out_dir, out_file)
        content_df.to_csv(out, index=False)


# --name 502_4 --ask_id 2019-00079 --rshp_id A0001 --prvcd_cd K0002 --cat_cd TWJHC200 
# --header_dir d:/data/out --header_file IF_DL_502_201900079_A0001_TWJHC200.txt
# --in_dir d:/data/in --in_file IF_DL_502_201900079_A0001_TWJHC200
# --out_dir d:/data/out

# python3.6 src/managefile.py 
# --name 502_4 --ask_id 2019-00079 --rshp_id A0001 --prvcd_cd K0002 --cat_cd TWJHC200 
# --header_dir /esb_nfs/2019/201900082/IF_DL_5 --header_file IF_DL_502_201900079_A0001_TWJHC200.txt 
# --in_dir /esb_nfs/esbmst/indigo/IF_DL_500/recv --in_file IF_DL_502_201900079_A0001_TWJHC200 
# --out_dir /esb_nfs/2019/201900079/IF_DL_5
# def _process_utterance(in_dir, in_file, out_dir, out_file, header_dir, header_file, cat_cd):


if __name__ == "__main__":
    import argparse
    from tqdm import tqdm
    
    parser = argparse.ArgumentParser()

    parser.add_argument('--name', type=str, default='502_4')
    parser.add_argument('--ask_id', type=str, default='2019-00079')
    parser.add_argument('--rshp_id', type=str, default='A0001')
    parser.add_argument('--prvcd_cd', type=str, default='K0002')
    parser.add_argument('--cat_cd', type=str, default='TWJHC200')
    parser.add_argument('--num_workers', type=int, default=None)
    parser.add_argument('--header_dir', type=str, default='d:/data')
    parser.add_argument('--header_file', type=str, default='None')
    parser.add_argument('--in_dir', type=str, default='d:/data/in')
    parser.add_argument('--in_file', type=str, default='IF_DL_502_201900079_A0001_TWJHC200')
    parser.add_argument('--out_dir', type=str, default='d:/data/out')
    #parser.add_argument('--out_file', type=str, default=None)
    config = parser.parse_args()
    # modifyfile(config, tqdm=tqdm)  

    fileList = [file for file in os.listdir(config.in_dir) if file.startswith(config.in_file)]
    for file in fileList:
        _process_utterance(config.in_dir, config.out_dir, file, 
            config.header_dir, config.header_file, config.cat_cd)



