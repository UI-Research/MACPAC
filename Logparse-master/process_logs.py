import os
import fnmatch
mypath = 'd:\\python\\'
myfiles = fnmatch.filter(os.listdir('.'), '*.log')
print(myfiles)

for filename in myfiles:
    filepath = 'd:\\Python\\' + filename
    print('\f',end='')
    print('============ Begin ',filepath,'===========')
    with open(filepath) as fp:  
       line = fp.readline()
       cnt = 1
       substring = "NOTE:"
       substring2 = "%INCLUDE"
       substring3 = "ERROR:"
       substring4 = "WARNING:"
       substring5 = "Physical Name"
       substring6 = "Libref"
       substring7 = "is in a format that is native to another host"
    #
    # is in a format that is native to another host
    # (substring in line and substring2 not in line) or (substring3 in line) or (substring4 in line) or (substring5 in line) or (substring6 in line)
    #
       while line:
           if (substring in line and substring2 not in line) or (substring3 in line) or (substring4 in line) or (substring5 in line) or (substring6 in line):
               print("[{}]: {}".format(cnt, line.strip()))      
           line = fp.readline()
           # print("[{}]: {}".format(cnt, line.strip()))
           cnt += 1
       print('============ End   ',filepath,'===========')    
