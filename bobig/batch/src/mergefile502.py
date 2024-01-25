import os, sys, csv

'''
# 심평원데이터 레코드마지막에 콤마가 있어 제거
# 신청번호에 - 가 없음
201800001 132
201800002 120
201800003 144
'''

print(sys.argv[1], sys.argv[2] )

askid = '201800001'
header = 'header/'+sys.argv[1]
content = '../2018/기관데이터/502/'+ askid + '/'+sys.argv[2]
out = 'data/'+sys.argv[2]


'''
header = 'syntax/'+'IF_DL_502_201800003_A0001_TBIPDV2018_1_1_144_20200531120000000.txt'
content = 'syntax/'+'IF_DL_502_201800003_A0001_TBIPDV2018_1_1_144_20200531120000000.txt'
out = 'syntax/'+'out.new'
'''

file_header = open(header, mode='rt', encoding='utf-8')
file_content = open(content, mode='rt', encoding='utf-8')
file_write = open(out, mode='wt', encoding='utf-8', newline='')
full_txt = ""
full_txt = file_header.read()
full_txt = full_txt +'\r'
file_write.write(full_txt)

writer = csv.writer(file_write, delimiter=',')
contentreader = csv.reader(file_content, delimiter=',')
for readRow in contentreader:
    newRow = []
    newRow.append(readRow[0][0:4]+'-'+readRow[0][4:9])
    #  리스트의 마지막은 제외
    newRow  = newRow + readRow[1:-1]

    writer.writerow(newRow)
    #print(newRow)
    #input()
    #contentlist.append(readRow)

file_header.close()
file_content.close()
file_write.close()

