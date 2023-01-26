
## **Overview**

In this project we are trying to predict long term delays by fusing static as well as real time data.

1) Steps needed to fuse real time data with static data.      
    1.1) Download the postgres sql driver jar from https://jdbc.postgresql.org/download/.        
    1.2) Place the jar file in HDFS or local as per convinence.  
    1.3) Replace the path in at line 76      "spark=SparkSession.builder.config('spark.jars','{jar path on hdfs}').appName('Fuse Table').getOrCreate()" with path where              you placed the jar file on hadoop.     
    1.4) Place the same jar in another directory where you will be placing the code file.  
    1.5) Get the fuseTable.py code file from git, place it in the same directory as where you placed the jar file in step 1.4.    
    1.6) Use the below command to run the file.   
             time $SPARK_HOME/bin/spark-submit --driver-class-path postgresql-42.4.0.jar Test.py 6612 output8   
             Here, 6612 is the route id and output8 is the directory where the fused dataset will be placed. Both of the parameters can be changed, where a diff route              id can be used in place of 6612.   
    1.7) The generated dataset can then be used to test the model    
 Note- DB might not be up by the time the code is tested, in such case you can drop us a mail and  we will start the DB server.    

2) Steps needed to train the model.      
    2.1) Get the TimeSeriesLSTM.ipynb from the git repository.     
    2.2) Open the Notebook in google collab, and upload the dataset in the collab runtime. We have provided a sample dataset "ds.csv" this can be used in case you are          not able to run fuseTable.py script.            
    2.3) After upload you just need to run the notebook in order to see delay prediction and model accuracy.            

## **Path Finding**

All commands to run the path finding notebook can be found in the notebook itself: shortest_path/demo.py