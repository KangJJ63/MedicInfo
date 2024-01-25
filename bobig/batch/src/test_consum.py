#from bin.ClassicDeId import get_columns
#from numpy.core.numeric import NaN
import pandas as pd
import numpy as np
#CFAM / HE_HT/HE_WT/HE_WC
a = pd.read_csv('D:/work/workspace/bobig/batch/src/IF_DL_504_2019A0031_B0001_HN17_ALL_1_1_20210201.txt')
a = a[['CFAM','HE_HT','HE_WT','HE_WC']]
print(a)

#temp = pd.DataFrame((pd.to_numeric(a['CFAM'], errors='coerce').astype('Int64')).fillna(-999))
temp = pd.DataFrame((pd.to_numeric(a['CFAM'], errors='coerce').astype(np.int64)).fillna(-999))
