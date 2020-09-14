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

start_time = time.time()
start_time_1 = start_time
sc = SparkContext('local[*]','App1')
sc.setLogLevel("ERROR")
input_file = sys.argv[1]
output_file = sys.argv[2]
no_partition = sys.argv[3]
#path_to_dir_with_JSON_files = '/Users/harimapatel/Desktop/USC_Course_work/DATA_Mining/Assignments/text.json'
#path_to_dir_with_JSON_files = '/Users/harimapatel/Downloads/review.json'
out_res = {}

def countpartition(iterator):
    yield sum(1 for _ in iterator)
    
def key_partition(keys):
    return hash(keys)

my_RDD_strings = sc.textFile(input_file)
jsonMappedRDD = my_RDD_strings.map(json.loads)

inn_res = {}
inn_res["n_partition"] = jsonMappedRDD.getNumPartitions()
inn_res["n_items"] = jsonMappedRDD.mapPartitions(countpartition).collect()
jsonLargestBusinessRDD = jsonMappedRDD.map(lambda p:(p["business_id"],1))
#print("Partitions structure: {}".format(jsonLargestBusinessRDD.glom().collect()))
jsonTP = jsonLargestBusinessRDD.combineByKey(lambda val : val, lambda a,b : a+b,lambda a,b : a+b).takeOrdered(10, key = lambda x: (-x[1],x[0]))
inn_res["exe_time"] = time.time() - start_time

out_res["default"] = inn_res
start_time = time.time()

jsonCustomizedRDD = jsonMappedRDD.map(lambda el:(el["business_id"], el)).partitionBy(int(no_partition), key_partition).map(lambda c:(c[1].get("business_id"),1))
#jsonLargestBusinessRDD = jsonRDD
jsonRDD = jsonCustomizedRDD.combineByKey(lambda val : val, lambda a,b : a+b,lambda a,b : a+b).takeOrdered(10, key = lambda x: (-x[1],x[0]))
inn2_res = {}
#print("Partitions structure: {}".format(jsonCustomizedRDD.glom().collect()))
inn2_res["n_partition"] = jsonCustomizedRDD.getNumPartitions()
inn2_res["n_items"] = jsonCustomizedRDD.mapPartitions(countpartition).collect()
inn2_res["exe_time"] = time.time() - start_time
out_res["Customized"] = inn2_res

with io.open(output_file, 'w', encoding='utf8') as outfile:
    str_ = json.dumps(out_res,indent=2)
    outfile.write(to_unicode(str_))

print("------------------------------------- %s seconds -------------------------------------------" % str(time.time() - start_time_1))
