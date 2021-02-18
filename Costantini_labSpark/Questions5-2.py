import time 
import pyspark
from pyspark import SparkContext
import matplotlib.pyplot as plt 
from collections import Counter
from random import sample
import numpy as np 

### Some parts of the code are inspired by the code provided for the first lab on Apache Spark 


### Setting a SparkContext and the adjusting the verbosity of the error messages : 
sc = SparkContext('local[1]')
sc.setLogLevel("ERROR")



### QUESTION 1 : What is the distribution of the machines according to their CPU capacity?

#Creation of the RDD and keeping the RDD in memory : 
machine_events = sc.textFile('../data/part-00000-of-00001.csv')
machine_events.cache()

CPU_capacity_index = 4 
machine_events_entries = machine_events.map(lambda x: x.split(','))

# RDD of ('CPU capacity', 'number of machines with this CPU capacity') : 
CPU_machines_distribution = machine_events_entries.map(lambda x: (x[CPU_capacity_index],1)).reduceByKey(lambda a,b: a+b).filter(lambda x: x[0] in ['0.25', '0.5', '1'])
# (The fourth CPU capacity value is non available (empty string) so I chose to filter it)

plt.bar(CPU_machines_distribution.keys().collect(), CPU_machines_distribution.values().collect(), edgecolor='black', color = 'blue')
plt.show()






### QUESTION 2 : What is the percentage of computational power lost due to maintenance (a machine went offline and reconnected later)?

# Analysis of the CPU capacity lost due to a REMOVE (1) event : 

# Computation of the total CPU capacity : 
total_CPU_capacity = machine_events_entries.map(lambda x: x[CPU_capacity_index]).filter(lambda x: x != '').reduce(lambda a,b: float(a)+float(b))
print(total_CPU_capacity) # Result = 19858.0

event_type_index = 2 

# Computation of CPU capacity on machines where REMOVE events occured : 
remove_CPU_capacity = machine_events_entries.map(lambda x: (x[event_type_index], x[CPU_capacity_index])).filter(lambda x: x[0] == '1').reduceByKey(lambda a,b: float(a)+float(b)).values().collect()
print(remove_CPU_capacity) # Result = 4764.0


# Computation of the total memory capacity : 
memory_capacity_index = 5

total_memory_capacity = machine_events_entries.map(lambda x: x[memory_capacity_index]).filter(lambda x: x != '').reduce(lambda a,b: float(a)+float(b))
print(total_memory_capacity) # Approximate result = 17996

# Computation of memory capacity on machines where REMOVE events occured : 
remove_memory_capacity = machine_events_entries.map(lambda x: (x[event_type_index], x[memory_capacity_index])).filter(lambda x: x[0] == '1').reduceByKey(lambda a,b: float(a)+float(b)).values().collect()
print(remove_memory_capacity) # Approximate result = 4404





### QUESTION 3 : On average, how many tasks compose a job?

# # Creation of the RDD and keeping the RDD in memory : 
task_events = sc.textFile('../data/task-events-part-00000-of-00500.csv')
task_events.cache()

task_events_index = 2
task_events_entries = task_events.map(lambda x: x.split(','))

jobs_ID = task_events_entries.map(lambda x: x[task_events_index])
jobs_ID_count = jobs_ID.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

jobs_ID_values = []

for value in jobs_ID_count.values().collect() : 
    jobs_ID_values.append(value)

average_nb_tasks = sum([int(i) for i in jobs_ID_values])/len(jobs_ID_values)
print(average_nb_tasks)




### QUESTION 4 : What can you say about the relation between the scheduling class of a job, the scheduling class of its tasks, and their priority?

# # Creation of the RDD and keeping the RDD in memory : 
job_events = sc.textFile('../data/job-events-part-00000-of-00500.csv')
job_events.cache()

job_events_entries = job_events.map(lambda x: x.split(','))

job_sched_class_index = 5
job_ID_index = 2 

# # RDD with the pairs ('job ID', 'scheduling class') for each job in the job_events table : 
ID_sched_class_pair_job = job_events_entries.map(lambda x: (x[job_ID_index], x[job_sched_class_index]))

task_sched_class_index = 7 

# # RDD with the pairs ('job ID', 'most frequent scheduling class') for each task in the task_events table : 
ID_sched_class_pair_task_most_common = task_events_entries.map(lambda x: (x[task_events_index], x[task_sched_class_index])).reduceByKey(lambda x,y: Counter(y).most_common()[0][0])

union = sc.union

# # RDD with pairs ('job ID', 'scheduling class' if the most frequent scheduling class of the tasks == the scheduling class of their job, '-1' otherwise)
task_job_sched_class_comparison = ID_sched_class_pair_job.union(ID_sched_class_pair_task_most_common).reduceByKey(lambda a,b: a if a==b else -1)

diff_sched_class_count = task_job_sched_class_comparison.groupBy(lambda x: x[1])

priority_task_index = 8 

# # list containing pairs ('scheduling task number', 'list of tasks priorities')
sched_class_priority_pair_task = task_events_entries.map(lambda x: (x[task_sched_class_index], x[priority_task_index])).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()

sched_class_priority_pair_task_dict = {}

for element in sched_class_priority_pair_task : 
    sched_class_priority_pair_task_dict[element[0]] = element[1]


for k in sched_class_priority_pair_task_dict.keys() : 
    plt.hist(sorted(sched_class_priority_pair_task_dict[k], key=Counter(sched_class_priority_pair_task_dict[k]).get, reverse=True), bins=range(len(set(sched_class_priority_pair_task_dict[k]))+2), edgecolor='black', color = 'red', align='right')
    plt.title(f'Repartition of priority codes for scheduling class = {k}')
    plt.show()





### QUESTION 5 :  Do tasks with low priority have a higher probability of being evicted ?

low_priority_tasks = task_events_entries.map(lambda x: (x[priority_task_index], x[task_sched_class_index])).filter(lambda x: int(x[0]) <= 3)

plt.hist(sorted(low_priority_tasks.values().collect(), key=Counter(low_priority_tasks.values().collect()).get, reverse=True), bins=range(len(set(low_priority_tasks.values().collect()))+2), edgecolor='black', color='red', align='right')
plt.title('Distribution of the scheduling class codes for low priority tasks (inferior or equal to 3)')
plt.show()

all_tasks_sched_class = task_events_entries.map(lambda x: (x[priority_task_index], x[task_sched_class_index]))

plt.hist(sorted(all_tasks_sched_class.values().collect(), key=Counter(all_tasks_sched_class.values().collect()).get, reverse=True), bins=range(len(set(all_tasks_sched_class.values().collect()))+2), edgecolor='black', color='red', align='right')
plt.title('Distribution of the scheduling class codes for all priorities tasks')
plt.show()




### QUESTION 6 : In general, do tasks from the same job run on the same machine?

machine_index = 4  

# # RDD of pairs ('job ID', 'machine ID') : 
job_ID_machine_pairs = task_events_entries.map(lambda x: (x[job_ID_index], x[machine_index])).groupByKey()

nb_of_machines_job = []

for element in job_ID_machine_pairs.values().collect() : 
    nb_of_machines_job.append(len(list(element)))

plt.hist(sorted(nb_of_machines_job, key=Counter(nb_of_machines_job).get, reverse=True), bins=range(len(set(nb_of_machines_job))+2), color='blue', align='right')
plt.title('Number of machines required to run a job')
plt.show()
print(Counter(nb_of_machines_job))





### QUESTION 7 : Are the tasks that request the more resources the one that consume the more resources?

mean_CPU_usage_rate_index = 5 

task_usage = sc.textFile('../data/task-usage-part-00000-of-00500.csv')
task_usage.cache()

task_usage_entries = task_usage.map(lambda x: x.split(','))

task_request_CPU_index = 9 
task_consumption_CPU_index = 5 
task_job_index = 3

def mean(l) : 
    return sum(l)/len(l)

# # RDD with the pairs ('job ID - task ID within job', 'mean CPU request') for each task : 
task_request_CPU = task_events_entries.map(lambda x: (x[job_ID_index] + '-' + x[task_job_index], x[task_request_CPU_index])).reduceByKey(lambda a,b: a + ',' + b).filter(lambda x: x[1][0] != ',')
task_request_CPU = task_request_CPU.map(lambda x: (x[0], mean([float(i) for i in x[1].split(',') ]) ))

# # RDD with the pairs ('job ID - task ID within job', 'mean CPU actual consumption') for each task : 
task_consumption_CPU = task_usage_entries.map(lambda x: (x[job_ID_index]+ '-' + x[task_job_index], x[task_consumption_CPU_index])).reduceByKey(lambda a,b: a + ',' + b).filter(lambda x: x[1][0] != ',')
task_consumption_CPU = task_consumption_CPU.map(lambda x: (x[0], mean([float(i) for i in x[1].split(',') ]) ))

# # RDD with ('job ID - task ID within job', ['mean CPU request', 'mean CPU consumption']) for each task 
# # (I only chose the tasks for which we have both information, i.e. tasks present in the two tables): 
task_CPU_request_vs_consumption = task_request_CPU.union(task_consumption_CPU).reduceByKey(lambda a,b: (str(a) + ',' + str(b)).split(',')).filter(lambda x: isinstance(x[1], list) and len(x[1]) == 2)

task_CPU_request_vs_consumption_values = task_CPU_request_vs_consumption.values().collect()

mean_CPU_request, mean_CPU_consumption = [x[0] for x in task_CPU_request_vs_consumption_values], [x[1] for x in task_CPU_request_vs_consumption_values]

rand_indices = sample(range(len(mean_CPU_request)),100)
mean_CPU_request_sample, mean_CPU_consumption_sample = [mean_CPU_request[i] for i in rand_indices], [mean_CPU_consumption[i] for i in rand_indices]

plt.scatter(mean_CPU_request_sample, mean_CPU_consumption_sample)
plt.xticks([])
plt.yticks([])
plt.xlabel('mean CPU request (CPU cores/s)')
plt.ylabel('mean CPU actual consumption (CPU cores/s)')
x = range(40)
y = 2*np.array(x)
plt.plot(x,y,color='red')
plt.show()





### QUESTION 8 : Is there a relation between the amount of resource consumed by tasks and their priority?

# # RDD of pairs ('job ID - task ID within job', 'most frequent priority code') : 
task_priority = task_events_entries.map(lambda x: (x[job_ID_index] + '-' + x[task_job_index], x[priority_task_index])).reduceByKey(lambda a,b: Counter(b).most_common()[0][0]).filter(lambda x: x[1][0] != ',')

# # RDD of ('job ID - task ID within job', ['most frequent priority code', 'CPU resources consumed']) :
priority_vs_resources = task_priority.union(task_consumption_CPU).reduceByKey(lambda a,b: (str(a) + ',' + str(b)).split(',')).filter(lambda x: isinstance(x[1], list) and len(x[1]) == 2)

priority_vs_resources_cumulative_values = priority_vs_resources.values().reduceByKey(lambda a,b: float(a)+float(b))

priority_values = priority_vs_resources_cumulative_values.keys().collect()
resources_values = priority_vs_resources_cumulative_values.values().collect()

plt.bar(priority_values, resources_values,edgecolor='black')
plt.show()





### Question 9 : Can we observe correlations between peaks of high resource consumption on some machines and task eviction events?

machine_ID_usage_index = 4
mean_CPU_usage_index = 5

# RDD with ('machine ID index', 'mean CPU consumption') : 
machine_CPU_consumption = task_usage_entries.map(lambda x: (x[machine_ID_usage_index], x[mean_CPU_usage_index])).reduceByKey(lambda a,b: a + ',' + b)

h1 = {}

for element in machine_CPU_consumption.collect() : 
    h1[element[0]] = element[1].split(',')
    h1[element[0]] = [float(i) for i in h1[element[0]]]

for k in h1.keys() : 
    h1.update({k : sum(h1[k])/len(h1[k])})

# Mean CPU consumption : 
m = []
for v in h1.values() : 
    m.append(v)

m.sort()

# 99th percentile of CPU consumption first value : 
last_percentile_value_CPU = m[int(99/100 * len(m))]

h1_2 = {}

for k in h1.keys() : 
    if h1[k] >= last_percentile_value_CPU : 
        h1_2[k] = h1[k]


machine_ID_events_index = 4
machine_event_type_index = 5

# RDD with ('machine ID', 'EVICT event') : 
machine_evict_events = task_events_entries.map(lambda x: (x[machine_ID_events_index], x[machine_event_type_index])).reduceByKey(lambda a,b: a + ',' + b)

h2 = {}

for element in machine_evict_events.collect() : 
    if element[0] in h1_2.keys() : 
        h2[element[0]] = [int(i) for i in element[1].split(',')]
        
l = list(h2.values())
l = [item for sublist in l for item in sublist]

plt.hist(l, align='mid')
plt.title('Event codes for the 99th percentile of CPU consumption')
plt.xlabel('Event codes')
plt.show()


# Same thing for the 90th percentile : 
last_percentile_value_CPU2 = m[int(90/100 * len(m))]

h1_3 = {}

for k in h1.keys() : 
    if h1[k] >= last_percentile_value_CPU2 : 
        h1_3[k] = h1[k]

h2_2 = {}

for element in machine_evict_events.collect() : 
    if element[0] in h1_3.keys() : 
        h2_2[element[0]] = [int(i) for i in element[1].split(',')]
        
l2 = list(h2_2.values())
l2 = [item for sublist in l2 for item in sublist]

plt.hist(l2, align='mid')
plt.title('Event codes for the 90th percentile of CPU consumption')
plt.xlabel('Event codes')
plt.show()


# Same thing for the 50th percentile : 
last_percentile_value_CPU3 = m[int(50/100 * len(m))]

h1_4 = {}

for k in h1.keys() : 
    if h1[k] >= last_percentile_value_CPU3 : 
        h1_4[k] = h1[k]

h2_3 = {}

for element in machine_evict_events.collect() : 
    if element[0] in h1_4.keys() : 
        h2_3[element[0]] = [int(i) for i in element[1].split(',')]
        
l3 = list(h2_3.values())
l3 = [item for sublist in l3 for item in sublist]

plt.hist(l3, align='mid')
plt.title('Event codes for the 50th percentile of CPU consumption')
plt.xlabel('Event codes')
plt.show()
