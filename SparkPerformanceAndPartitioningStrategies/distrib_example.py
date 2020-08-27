import random as rand
import pandas as pd

def newDf(data,values_cnt=50,partition_cnt=4):
  src=[]
  if (isinstance(data,list)):
    raise ValueError(f"data type must be a list : {type(data)} ")
  for i in range(values_cnt):
    src.append([data[int(rand.triangular(0,len(data),mode=len(data)))], rand.randrange(0,1000)])

  return spark.createDataFrame(src, schema=("col1 string, col2 integer")).repartition(partition_cnt,"col1")
  
  def df_details(df):
  #df.explain()
  part_num=0
  row_num=0
  
  print(f"#Partitions = {len(df.rdd.glom().collect())}")
  for p in df.rdd.glom().collect():
    print(f"P{part_num}")
    for rowInPart in p:
      print(f"  Row {row_num}:{rowInPart}")
      row_num=row_num+1
    part_num=part_num+1
    
def df_details_2(df):
  #df.explain()
  part_num=0
  
  print(f"#Partitions = {len(df.rdd.glom().collect())}")
  for p in df.rdd.glom().collect():
    row_num=0
    values=[]
    print(f"----------- P{part_num} -----------")
    for rowInPart in p:
      if(rowInPart[0] not in values): 
        values.append(rowInPart[0])
      row_num=row_num+1
    print(f"  #Rows in P{part_num}  = {row_num}")
    print(f"  Values in P{part_num} = {values if values else 'N/A'}")
    part_num=part_num+1

def df_details_pd(df):
  part_num=0
  #If there is too much data, a sample can help
  #parts=df.rdd.glom().sample(withReplacement=False, fraction=0.1).collect()
  parts=df.rdd.glom().collect()
  partCount=len(parts)

  res=pd.DataFrame(data=None, index=None, columns=('value','cnt'))
  for p in parts:
    row_num=0
    values=[]
    for rowInPart in p:
      if(rowInPart[0] not in values): 
        values.append(rowInPart[0])
      row_num=row_num+1
    
    s="Part#: {}\n Values: \n {}".format(part_num, (",\n".join(values) if values else "N/A" ))
    res=res.append(pd.DataFrame([[s,row_num]], columns=('value','cnt')),ignore_index=True)
    part_num=part_num+1
  return res
  
  
