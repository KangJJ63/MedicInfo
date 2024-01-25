import os, sys, csv

'''
# 결핵데이터 dataset명 없음
'''

print(sys.argv[1], sys.argv[2])

dataset = sys.argv[1]
content = 'D:/work/2019/in/'+sys.argv[2]
out = 'D:/work/2019/out/'+sys.argv[2]

file_content = open(content, mode='rt', encoding='utf-8')
file_write = open(out, mode='wt', encoding='utf-8', newline='')

writer = csv.writer(file_write, delimiter=',')
#contentlist = []
contentreader = csv.reader(file_content, delimiter=',')
first = 1
for readRow in contentreader:
    newRow = []
    newRow.extend(readRow[0:4])
    if first == 1:
        newRow.extend(['CAT_CD'])
        first += 1
    else:    
        newRow.extend([dataset])   
    #  리스트의 마지막은 제외
    newRow  = newRow + readRow[4:]
    writer.writerow(newRow)
    #print(newRow)
    #input()
    #contentlist.append(readRow)

file_content.close()
file_write.close()

