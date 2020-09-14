import json
from pyspark import SparkContext,SparkConf
import os
import sys
import io
import time

start_time = time.time()
import io
try:
    to_unicode = unicode
except NameError:
    to_unicode = str

conf = SparkConf().set('spark.driver.cores',4)
sc = SparkContext('local[*]','App1',conf = conf)
sc.setLogLevel("ERROR")
input_file = sys.argv[1]
output_file = sys.argv[2]
path_to_dir_with_JSON_files = input_file
res = {}

jsonRDD = sc.textFile(path_to_dir_with_JSON_files).map(json.loads).map(lambda x : (x['user_id'],x['business_id'],x['date']))
jsonRDD = jsonRDD.repartition(8)
res["n_review"] = jsonRDD.count()

res["n_review_2018"] = jsonRDD.filter(lambda line : line[2][0:4]=="2018").count()

jsonDistinctUsers = jsonRDD.map(lambda c : c[0])
res["n_user"] = jsonDistinctUsers.distinct().count()

res["top10_user"] = [list(ele) for ele in jsonDistinctUsers.map(lambda z : (z,1)).combineByKey(lambda val : val, lambda a,b : a+b,lambda a,b : a+b).takeOrdered(10, key = lambda x: (-x[1],x[0]))]

jsonDistinctBusinessRDD = jsonRDD.map(lambda x:x[1])
res["n_business"] = jsonDistinctBusinessRDD.distinct().count()

res["top10_business"]  = [list(ele) for ele in jsonDistinctBusinessRDD.map(lambda p : (p,1)).combineByKey(lambda val : val, lambda a,b : a+b,lambda a,b : a+b).takeOrdered(10, key = lambda x: (-x[1],x[0]))]

print("----------------------------------------seconds-------------------------------------------- = " + str(time.time()-start_time))

with io.open(output_file, 'w', encoding='utf8') as outfile:
    str_ = json.dumps(res,indent=4)
    outfile.write(to_unicode(str_))
