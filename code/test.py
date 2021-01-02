# import time 
# import pyspark
# from pyspark import SparkContext
# import matplotlib.pyplot as plt 

# sc = SparkContext('local[1]')
# sc.setLogLevel("ERROR")


# job_events = sc.textFile('../data/job-events-part-00000-of-00500.csv')
# job_events.cache()

# job_events_entries = job_events.map(lambda x: x.split(','))

# job_sched_class_index = 5
# job_ID_index = 2 

# ID_sched_class_pair = job_events_entries.map(lambda x: (x[job_ID_index], x[job_sched_class_index]))

from collections import Counter 
a = [1, 1, 2, 3, 3, 3, 4, 4, 4, 5, 5, 5, 5]
print(sorted(a, key=Counter(a).get, reverse=True))

