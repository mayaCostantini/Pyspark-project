import time 
import pyspark
from pyspark import SparkContext
import matplotlib.pyplot as plt 
from collections import Counter

### Setting a SparkContext and the adjusting the verbosity of the error messages : 
sc = SparkContext('local[1]')
sc.setLogLevel("ERROR")

### Question 1 : What is the distribution of the machines according to their CPU capacity?

#Creation of the RDD and keeping the RDD in memory : 
# machine_events = sc.textFile('../data/part-00000-of-00001.csv')
# machine_events.cache()

# machine_events_index = 4 
# machine_events_entries = machine_events.map(lambda x: x.split(','))

# CPU_machines_distribution = machine_events_entries.map(lambda x: x[machine_events_index])
# plt.hist(CPU_machines_distribution.collect())
# plt.show()



### Question 3 : On average, how many tasks compose a job?

# Creation of the RDD and keeping the RDD in memory : 
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


### Question 4 : What can you say about the relation between the scheduling class of a job, the schedulingclass of its tasks, and their priority?

# Creation of the RDD and keeping the RDD in memory : 
job_events = sc.textFile('../data/job-events-part-00000-of-00500.csv')
job_events.cache()

job_events_entries = job_events.map(lambda x: x.split(','))

job_sched_class_index = 5
job_ID_index = 2 

# RDD with the pairs ('job ID', 'scheduling class') for each job in the job_events table : 
ID_sched_class_pair_job = job_events_entries.map(lambda x: (x[job_ID_index], x[job_sched_class_index]))

task_sched_class_index = 7 

# RDD with the pairs ('job ID', 'most frequent scheduling class') for each task in the task_events table : 
ID_sched_class_pair_task_most_common = task_events_entries.map(lambda x: (x[task_events_index], x[task_sched_class_index])).reduceByKey(lambda x,y: Counter(y).most_common()[0][0])

union = sc.union

# RDD with pairs ('job ID', '')
task_job_sched_class_comparison = ID_sched_class_pair_job.union(ID_sched_class_pair_task_most_common).reduceByKey(lambda a,b: a if a==b else -1)

diff_sched_class_count = task_job_sched_class_comparison.groupBy(lambda x: x[1])

# for element in diff_sched_class_count.collect() : 
#     print(element)

priority_task_index = 8 

sched_class_priority_pair_task = task_events_entries.map(lambda x: (x[task_sched_class_index], x[priority_task_index])).groupBy(lambda x: x[0])

sched_class_priorities = {}

for element in sched_class_priority_pair_task.collect() : 
    if element[0] in sched_class_priorities : 
        sched_class_priorities[element[0]] += list(element[1])
        print(element[0], element[1])
    else : 
        sched_class_priorities[element[0]] = list(element[1])

# print(sched_class_priorities)
### Question 5 : 