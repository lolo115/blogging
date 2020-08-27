import random as rand
import pandas as pd

def newDf(data,values_cnt=50,partition_cnt=4):
  src=[]
  if (isinstance(data,list)):
    raise ValueError(f"data type must be a list : {type(data)} ")
  for i in range(values_cnt):
    src.append([data[int(rand.triangular(0,len(data),mode=len(data)))], rand.randrange(0,1000)])

  return spark.createDataFrame(src, schema=("col1 string, col2 integer")).repartition(partition_cnt,"col1")
