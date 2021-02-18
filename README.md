# Analyzing data with Spark 

## Presentation of the project and setup
This project was built with MacOS Catalina version 10.15.1, Python 3.8 and the Visual Studio Code environment.

The goal of this project is to use the Apache Spark Python API to analyze a public Google dataset from 2011 containing information about a cluster of 12500 machines, recorded for a duration of 29 days (size 41GB). 
The data is available on Google Cloud via the GSUtil tool, a Python application to get a command line access to Cloud Storage.

### Setup
The general instructions to install GSUtil are available via the following link : 

https://cloud.google.com/storage/docs/gsutil_install

To download the archive : https://cloud.google.com/storage/docs/gsutil_install# alt-install

The GSUtil documentation can be accessed via https://cloud.google.com/storage/docs/gsutil 
 
The command ```gsutil ls gs://clusterdata-2011-2/``` allows to see the available files of the dataset ```clusterdata-2011-2```. 
Executing ```gsutil cp gs://clusterdata-2011-2/machine_events/part-00000-of-00001.csv.gz ./``` will copy the compressed file ```part-00000-of-00001.csv.gz``` in the current working directory. 

### Content of the dataset 

More information about the tables and the content of the dataset is available in the following document : https://drive.google.com/file/d/0B5g07T_gRDg9Z0lsSTEtTWtpOW8/view



