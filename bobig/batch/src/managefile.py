import os, sys, csv
import argparse
from multiprocessing import cpu_count
from tqdm import tqdm
import importlib
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)


'''
--name 502_3 --ask_id 2019-00082 --rshp_id A0001 --prvcd_cd K0002 --cat_cd TWJHC200 
--header_dir d:/data --header_file IF_DL_502_201900082_A0001_TWJHC200.txt
--in_dir d:/data/in --in_file IF_DL_502_201900082_A0001_TWJHC200
--out_dir d:/data/out

--header_dir /esb_nfs/2019/201900082 --header_file IF_DL_502_201900082_A0001_TWJHC200.txt
--in_dir /esb_nfs/esbmst/indigo/IF_DL_500/recv --in_file IF_DL_502_201900082_A0001_TWJHC200
--out_dir /esb_nfs/2019/201900082/IF_DL_5
'''

def managefile(mod, config):
    os.makedirs(config.out_dir, exist_ok=True)  
    job_result = mod.modifyfile(config, tqdm=tqdm)  
    job_report(job_result)

def job_report(job_result):
    print(job_result)


def main():
    parser = argparse.ArgumentParser()
    
    parser.add_argument('--name', type=str, default=None)
    parser.add_argument('--ask_id', type=str, default=None)
    parser.add_argument('--rshp_id', type=str, default=None)
    parser.add_argument('--prvcd_cd', type=str, default=None)
    parser.add_argument('--cat_cd', type=str, default=None)
    parser.add_argument('--num_workers', type=int, default=None)
    parser.add_argument('--header_dir', type=str, default=None)
    parser.add_argument('--header_file', type=str, default=None)
    parser.add_argument('--in_dir', type=str, default=None)
    parser.add_argument('--in_file', type=str, default=None)
    parser.add_argument('--out_dir', type=str, default=None)
    #parser.add_argument('--out_file', type=str, default=None)
    config = parser.parse_args()

    if config.out_dir is None:
        exit
    if config.in_dir is None:
        exit

    n_ws = cpu_count() if config.num_workers is None else config.num_workers
    config.num_workers = n_ws;

    mod = importlib.import_module('manage.{}'.format(config.name))
    managefile(mod, config)

if __name__ == "__main__":
    main()
