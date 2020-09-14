#import json
#from pyspark import SparkContext
#import os
#import io
#import time
#import sys
#import io
#
#try:
#    to_unicode = unicode
#except NameError:
#    to_unicode = str
#
#sc = SparkContext('local[*]','App1')
#sc.setLogLevel("ERROR")
#input_file_business = sys.argv[1]
#input_file_review = sys.argv[2]
#output_file_1 = sys.argv[3]
#output_file_2 = sys.argv[4]
#
#res = {}
#my_RDD_strings = sc.textFile(input_file_review)
#jsonRDD1 = my_RDD_strings.map(json.loads)
#
#jsonBSRDD = jsonRDD1.map(lambda a:(a.get("business_id"),a.get("stars"))).combineByKey(lambda value:(value,1),lambda x, value: (x[0] + value, x[1] + 1),
#lambda x, y: (x[0] + y[0], x[1] + y[1]))
##print("------------------------***********************Review mapping-------------------"+str(jsonBSRDD.collect()))
#
#my_RDD_strings = sc.textFile(input_file_business)
#jsonRDD2 = my_RDD_strings.map(json.loads)
#
#jsonBCRDD = jsonRDD2.map(lambda a: (a.get("business_id"),a.get("city")))
##print("------------------------***********************Business mapping-------------------"+str(jsonBCRDD.collect()))
#
#jsonJoinedRDD = jsonBSRDD.leftOuterJoin(jsonBCRDD)
##print("------------------------***********************Left outer join-------------------"+str(jsonJoinedRDD.collect()))
#
#jsonFinalRDD = jsonJoinedRDD.map(lambda x: (x[1][1], x[1][0])).combineByKey(lambda value: (value[0],value[1]), lambda x, value: (x[0] + value[0], x[1] + value[1]), lambda x, y: (x[0] + y[0], x[1] + y[1]))
##print("----------------*************************Json Final RDD-------------------------------------------" + str(jsonFinalRDD.collect()))
#
#average = jsonFinalRDD.map(lambda label : (label[0], label[1][0]/label[1][1])).sortBy(lambda x : (-x[1],x[0]))
##print("----------------*************************Json average-------------------------------------------" + str(average.collect()))
#
#file = open(output_file_1,"w")
#file.write("city,stars\n")
#for ls in average.collect():
#    file.write(str(ls[0])+","+str(ls[1]))
#    file.write("\n")
#file.close()
#
#jsonRDD2 = jsonRDD2.map(lambda x : (x.get("city"),x.get("stars"))).distinct()
#start_time = time.time()
#tup = jsonRDD2.collect()
#tup.sort(key = lambda x: (-x[1],x[0]))
#print(tup[:10])
#res["m1"] = time.time()-start_time
#start_time = time.time()
#print(jsonRDD2.sortBy(lambda x:(-x[1],x[0])).take(10))
#res["m2"] = time.time()-start_time
#
#with io.open(output_file_2, 'w', encoding='utf8') as outfile:
#    str_ = json.dumps(res,indent=2)
#    outfile.write(to_unicode(str_))

import json
from pyspark import SparkContext
import os
import io
import time
import sys
import io

try:
    to_unicode = unicode
except NameError:
    to_unicode = str
    
sc = SparkContext('local[*]','App1')
sc.setLogLevel("ERROR")
input_file_business = sys.argv[1]
input_file_review = sys.argv[2]
output_file_1 = sys.argv[3]
output_file_2 = sys.argv[4]

res = {}
my_RDD_strings = sc.textFile(input_file_review)
jsonRDD1 = my_RDD_strings.map(json.loads)

jsonBSRDD = jsonRDD1.map(lambda a:(a.get("business_id"),a.get("stars"))).combineByKey(lambda value:(value,1),lambda x, value: (x[0] + value, x[1] + 1),
lambda x, y: (x[0] + y[0], x[1] + y[1]))
my_RDD_strings = sc.textFile(input_file_business)
jsonRDD2 = my_RDD_strings.map(json.loads)

jsonBCRDD = jsonRDD2.map(lambda a: (a.get("business_id"),a.get("city")))

jsonJoinedRDD = jsonBSRDD.leftOuterJoin(jsonBCRDD)

jsonFinalRDD = jsonJoinedRDD.map(lambda x: (x[1][1], x[1][0])).combineByKey(lambda value: (value[0],value[1]), lambda x, value: (x[0] + value[0], x[1] + value[1]), lambda x, y: (x[0] + y[0], x[1] + y[1]))

average = jsonFinalRDD.map(lambda label : (label[0], label[1][0]/label[1][1]))

start_time_1 = time.time()
tup = average.collect()
tup.sort(key = lambda x: (-x[1],x[0]))
print(tup[:10])
res["m1"] = time.time()-start_time_1
start_time_2 = time.time()
SortedRDD = average.sortBy(lambda x:(-x[1],x[0]))
print(SortedRDD.take(10))
res["m2"] = time.time()-start_time_2

file = open(output_file_1,"w")
file.write("city,stars\n")
for ls in SortedRDD.collect():
    file.write(str(ls[0])+","+str(ls[1]))
    file.write("\n")
file.close()

with io.open(output_file_2, 'w', encoding='utf8') as outfile:
    str_ = json.dumps(res,indent=2)
    outfile.write(to_unicode(str_))
