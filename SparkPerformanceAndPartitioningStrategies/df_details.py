import random as rand
import pandas as pd

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
