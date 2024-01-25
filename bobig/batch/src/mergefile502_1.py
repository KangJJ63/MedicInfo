import os, sys, csv

'''
# 심평원데이터 레코드마지막에 콤마가 있어 제거
# 201800004A0001K00023E034EEF7DDFE9A9A5F9E1B862DD8B9B7CF72C0CD87B03957F40F4E153393427
# 신청번호에 - 가 없음
# dataset명도 없음
'''

print(sys.argv[1], sys.argv[2], sys.argv[3])

header = 'header/'+sys.argv[1]
dataset = sys.argv[3]
content = '../2018/기관데이터/502/201800004/'+sys.argv[2]
out = 'data/'+sys.argv[2]

file_header = open(header, mode='rt', encoding='utf-8')
file_content = open(content, mode='rt', encoding='utf-8')

file_write = open(out, mode='wt', encoding='utf-8', newline='')
full_txt = ""
full_txt = file_header.read()
full_txt = full_txt +'\r'
file_write.write(full_txt)

writer = csv.writer(file_write, delimiter=',')
#contentlist = []
contentreader = csv.reader(file_content, delimiter=',')

for readRow in contentreader:
    newRow = []
    newRow.append(readRow[0][0:4]+'-'+readRow[0][4:9])
    newRow.append(readRow[0][9:14])
    newRow.append(readRow[0][14:19])
    newRow.append(readRow[0][19:83])
    newRow.append(dataset)
    #  리스트의 마지막은 제외
    newRow  = newRow + readRow[1:-1]
    writer.writerow(newRow)
    #print(newRow)
    #input()
    #contentlist.append(readRow)

file_header.close()
file_content.close()
file_write.close()

