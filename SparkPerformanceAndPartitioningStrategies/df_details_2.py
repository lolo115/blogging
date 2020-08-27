import pandas as pd

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
