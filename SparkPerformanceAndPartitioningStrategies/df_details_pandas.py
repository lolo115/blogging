import pandas as pd

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
