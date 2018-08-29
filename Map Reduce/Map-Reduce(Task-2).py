
# coding: utf-8

# In[2]:


import csv
data1 = sc.textFile("C:\\Users\\Ikshita\\Downloads\\ml-latest-small\\ratings.csv")
header1 = data1.first()
rdd1 = data1.filter(lambda x:x!=header1)
rdd1 = rdd1.map(lambda x: x.split(','))
rdd1 = rdd1.map(lambda x : (float(x[1]),float(x[2])))
rdd1.take(20)


# In[3]:


data2 = sc.textFile("C:\\Users\\Ikshita\\Downloads\\ml-latest-small\\tags.csv")
header2 = data2.first()
rdd2 = data2.filter(lambda x:x!=header2)
rdd2 = rdd2.map(lambda x:x.replace("'",""))


# In[4]:


rdd2 = rdd2.map(lambda x: x.split(','))


# In[5]:


rdd2 = rdd2.map(lambda x : (float(x[1]),x[2]))
rdd2.take(20)


# In[6]:


merge = rdd2.join(rdd1)
merge.take(20)


# In[7]:


key_value =merge.map(lambda xy: xy[1])


# In[8]:


key_value.take(50)


# In[ ]:


reduce_input = key_value.groupByKey().sortByKey(ascending = False).map(lambda x: (x[0],(list(x[1]))))


# In[ ]:


result = reduce_input.map(lambda xy: (xy[0],(sum(xy[1]))/len(xy[1])))


# In[ ]:


result.take(50)


# In[ ]:


with open('C:\\Users\\Ikshita\\Downloads\\ml-latest-small\\mapreduce_.csv', 'w')  as file:
    w = csv.writer(file)
    w.writerow(['tag','rating_avg'])
    for i in result.collect():
        final = str(i[0])+ ","+str(i[1])
        file.write(final)
        file.write('\n')

