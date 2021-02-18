import pyspark
from pyspark import SparkContext
import matplotlib.pyplot as plt 
from operator import add 


### Setting a SparkContext and the adjusting the verbosity of the error messages : 
sc = SparkContext('local[1]')
sc.setLogLevel("ERROR")

#Creation of the RDD and keeping the RDD in memory : 
machine_events = sc.textFile('../data/part-00000-of-00001.csv')
machine_events.cache()

CPU_capacity_index = 4 
memory_capacity_index = 5 

machine_events_entries = machine_events.map(lambda x: x.split(','))

# Creating the RDD of ('CPU capacity', 'Memory capacity') for each machine identified by a machine ID : 
CPU_vs_memory = machine_events_entries.map(lambda x: (x[CPU_capacity_index], x[memory_capacity_index])).filter(lambda x: x != ('','')) # Filtering the unavailable values ('','')

machines_count = CPU_vs_memory.map(lambda x: (x,1)).reduceByKey(add)

x, y = [], []

for element in machines_count.keys().collect() : 
    x.append(float(element[0]))
    y.append(float(element[1]))
    
z = []

for element in machines_count.values().collect() : 
    z.append(float(element))

plt.scatter(x,y,s=z, alpha=0.5)
plt.xlabel('CPU capacity')
plt.ylabel('Memory capacity')
plt.show()