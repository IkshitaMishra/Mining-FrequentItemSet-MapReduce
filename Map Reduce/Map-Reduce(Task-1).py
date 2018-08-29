
# coding: utf-8

# In[187]:


data = sc.textFile("C:\\Users\\Ikshita\\Downloads\\ml-latest-small\\ratings.csv")


# In[188]:


data.take(20)


# In[189]:


header  = data.first()


# In[190]:


non_header_data = data.filter(lambda x:x!=header)


# In[191]:


split_data = non_header_data.map(lambda x: x.split(','))


# In[192]:


key_value1 = split_data.map(lambda x: (float(x[1]),float(x[2])))


# In[193]:


reduceinput = key_value1.groupByKey().map(lambda x: (x[0], (list(x[1])))).sortByKey(True)


# In[194]:


result = reduceinput.map(lambda xy: (float(xy[0]),(float(sum(xy[1]))/float(len(xy[1])))))


# In[195]:


result.take(100)


# In[199]:


with open('C:\\Users\\Ikshita\\Downloads\\ml-latest-small\\mapreduce_proj.csv', 'w')  as file:
    for i in result.collect():
        final = str(i[0])+ ","+str(i[1])
        file.write(final)
        file.write('\n')

   

