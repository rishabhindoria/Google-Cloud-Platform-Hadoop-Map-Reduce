# Google-Cloud-Platform-Hadoop-Map-Reduce
•	Objective : To determine which words occur how many times in a dataset of textbooks
      Dataset size: 1GB
      
•	Created a Google Dataproc Cluster

•	Created a master and 3 worker nodes. Each node’s configuration was n1-standard-4 4vCPU 15 GB memory.

•	Mapper class in java program was used to output key:value pairs wherein key was words and value was the textbook_id in which that particular word occurred.

•	Reducer class in java program was used to grab the key:value pairs emitted by Mapper class to create the final output line for each word which stated which word occurred how many times in each of the textbook_id in the dataset(1GB) of textbooks.

•	The java program took 1hr to run on 1GB dataset uploaded on Google Cloud Platform to create the final output.
