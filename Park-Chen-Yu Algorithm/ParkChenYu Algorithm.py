
# coding: utf-8

# In[3]:


from itertools import combinations
data = sc.textFile("C:\\Users\\Ikshita\\Desktop\\inputdata.txt")


# In[4]:


data.take(10)


# In[5]:



input_list = data.map(lambda x:x.split(','))
input_list = input_list.map(lambda x:sorted(x))
input_list.take(10)


# In[6]:


#count each pair in data
pairs_input = input_list.map(lambda x: list(combinations(x,2)))
pairs_input =pairs_input.flatMap(lambda x:x)
count_pair =pairs_input.map(lambda x:(x,1)).sortByKey()
count_pair_data=count_pair.reduceByKey(lambda x,y:x+y)
count_pair_data.take(10)


# In[7]:


#count of each item
data = input_list.flatMap(lambda x:x)
data_map = data.map(lambda x:(x,1))
data_count = data_map.reduceByKey(lambda x,y:x+y)
data_count.take(15)

#support =4
freq_item = data_count.filter(lambda x: x[1]>=4).sortByKey()
freq_item=freq_item.map(lambda x:x[0])
freq_item.take(10)


# In[8]:


#finding pairs
combination = freq_item.map(lambda x: (1,x))
combination= combination.groupByKey().map(lambda x: (x[0],(list(x[1]))))
comb = combination.map(lambda x: (x[0],(list(combinations(x[1],2)))))
comb = comb.flatMap(lambda x:x[1])
comb.take(25)


# In[9]:


#hash
comb=comb.zipWithIndex()
comb.take(100)
pairs_with_bucketno = comb.map(lambda x:(x[0],(x[1]%20)))
pairs_with_bucketno.take(30)
pairs_with_buc = pairs_with_bucketno.map(lambda xy:(xy[1],xy[0]))


# In[10]:


bucketcount = pairs_with_bucketno.join(count_pair_data)
bucketcount.take(20)


# In[11]:


bucket_freq=bucketcount.map(lambda x:(x[1][0],(x[0],x[1][1]))).sortByKey()
bucket_1 = bucket_freq.map(lambda x: (x[0],(x[1][1]))).groupByKey().sortByKey().map(lambda x : (x[0],(sum(x[1])))).filter(lambda x:x[1]>=4)
freq_bucket = bucket_1.map(lambda x: x[0]).collect()
print(freq_bucket)


# In[12]:


#buck = pairs_with_bucketno.map(lambda x:x[1])
bitvector = pairs_with_bucketno.map(lambda x:(x,1 if x[1] in freq_bucket else 0 ))
bits_pairs= bitvector.map(lambda x:(x[0][0],x[1]))
bits_pairs.take(10)
#pairs_with_bucketno = pairs_with_bucketno.join(bitvector)
#pairs_with_bucketno.take(20)


# In[13]:


bits_1 = bits_pairs.filter(lambda x: (x[1] ==1))
bits_1.take(20)
freq_itemset_2 = bits_1.map(lambda x : list(x[0]))
freq_itemset_2.take(20)


# In[14]:


with open('C:\\Users\\Ikshita\\Desktop\\pcy_output.txt', 'w')  as file:
    freq_1=[]
    freq_2=[]
    for i in freq_item.collect():
        freq_1+=i
    file.write(str(freq_1))
    file.write("\n")
    freq_2+=freq_itemset_2.collect()
    file.write(str(freq_2))

