from matplotlib import pyplot as plt
from matplotlib import cm
import pandas as pd

names=('FR','DE','US','GB','IT','ES')
names=('aaaaaaaaaa','bbbbbbbbbb',
       'cccccccccc','dddddddddd',
       'eeeeeeeeee','ffffffffff',
       'ggggggggggg','hhhhhhhhhh')
nb_of_values=100
nb_part=8
df=newDf(data=names,
         values_cnt=nb_of_values,
         partition_cnt=nb_part)
         
def myPartitionFunction(k):
    return names.index(k)

df_mod=df.rdd.map(lambda x: (x[0], x)).partitionBy(nb_part,myPartitionFunction).toDF()
pddf_res=df_details_pd(df_mod)

if __name__=="__main__":
  
  cmap = cm.get_cmap('tab20') 

  fig,ax=plt.subplots(2,1,figsize=(20,30))
  pddf_res.plot(kind='bar',x='value', rot=0, figsize=(20,15), ax=ax[0])
  pddf_res.plot(kind='pie', y='cnt', ax=ax[1], colormap=cmap)
  ax[1].legend( pddf_res.value, loc='lower center', ncol=len(pddf_res.value))
