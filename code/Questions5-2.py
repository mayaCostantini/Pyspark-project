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

# RDD with pairs ('job ID', 'scheduling class' if the most frequent scheduling class of the tasks == the scheduling class of their job, '-1' otherwise)
task_job_sched_class_comparison = ID_sched_class_pair_job.union(ID_sched_class_pair_task_most_common).reduceByKey(lambda a,b: a if a==b else -1)

diff_sched_class_count = task_job_sched_class_comparison.groupBy(lambda x: x[1])

# for element in diff_sched_class_count.collect() : 
#     print(element)

priority_task_index = 8 

# list containing pairs ('scheduling task number', 'list of tasks priorities')
sched_class_priority_pair_task = task_events_entries.map(lambda x: (x[task_sched_class_index], x[priority_task_index])).groupByKey().map(lambda x : (x[0], list(x[1]))).collect()

sched_class_priority_pair_task_dict = {}

for element in sched_class_priority_pair_task : 
    sched_class_priority_pair_task_dict[element[0]] = element[1]


# for k in sched_class_priority_pair_task_dict.keys() : 
#     plt.hist(sorted(sched_class_priority_pair_task_dict[k], key=Counter(sched_class_priority_pair_task_dict[k]).get, reverse=True), bins=range(len(set(sched_class_priority_pair_task_dict[k]))+2), edgecolor='black', color = 'red', align='right')
#     plt.title(f'Repartition of priority codes for scheduling class = {k}')
#     plt.show()



### Question 5 :  Do tasks with low priority have a higher probability of being evicted ?

low_priority_tasks = task_events_entries.map(lambda x: (x[priority_task_index], x[task_sched_class_index])).filter(lambda x: int(x[0]) <= 3)

# plt.hist(sorted(low_priority_tasks.values().collect(), key=Counter(low_priority_tasks.values().collect()).get, reverse=True), bins=range(len(set(low_priority_tasks.values().collect()))+2), edgecolor='black', color='red', align='right')
# plt.title('Distribution of the scheduling class codes for low priority tasks (inferior or equal to 3)')
# plt.show()

all_tasks_sched_class = task_events_entries.map(lambda x: (x[priority_task_index], x[task_sched_class_index]))

# plt.hist(sorted(all_tasks_sched_class.values().collect(), key=Counter(all_tasks_sched_class.values().collect()).get, reverse=True), bins=range(len(set(all_tasks_sched_class.values().collect()))+2), edgecolor='black', color='red', align='right')
# plt.title('Distribution of the scheduling class codes for all priorities tasks')
# plt.show()


### Question 6 : In general, do tasks from the same job run on the same machine?

machine_index = 4  

# RDD of pairs ('job ID', 'machine ID') : 
job_ID_machine_pairs = task_events_entries.map(lambda x: (x[job_ID_index], x[machine_index])).groupByKey()

nb_of_machines_job = []

for element in job_ID_machine_pairs.values().collect() : 
    nb_of_machines_job.append(len(list(element)))

# plt.hist(sorted(nb_of_machines_job, key=Counter(nb_of_machines_job).get, reverse=True), bins=range(len(set(nb_of_machines_job))+2), color='blue', align='right')
# plt.title('Number of machines required to run a job')
# plt.show()
# print(Counter(nb_of_machines_job))