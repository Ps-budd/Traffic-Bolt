# Traffic Bolt   ![Pipeline](https://github.com/Ps-budd/Traffic-Bolt/blob/master/Images/Traffic-Bolt.jpg)

### [Presentation Slides](https://docs.google.com/presentation/d/1snsZjZkWUjwdy_0o7bam-rFQLeCnHkwbf63Rf6z_234/edit#slide=id.g9dc222c2d3_0_0)
* A analytical pipeline that provide data driven insights on city traffic.*
1. Weather condition affecting city traffic.
2. Crash/incidents happening on road affecting the city traffic.
3. Fatal rate on street due to crash.
4. Busiest streets.
5. Traffic density on geo map.
# Table of contents

1. [Problem](README.md#Problem)
2. [App_Dashboard](README.md#app_dashboard)
3. [Data Processing](README.md#data_processing)
4. [Ingestion](README.md#Ingestion)
5. [sample data](README.md#sample_data)
5. [Enviroment setup](README.md#architecture_setup)
6. [Contact](README.md#Contact)

 
### [Product- DASHBOARDS](https://public.tableau.com/profile/aditya.dubey3253#!/)
```angular2
      https://public.tableau.com/profile/aditya.dubey3253#!/
```
### [1.Traffic analysis on geo map](https://public.tableau.com/profile/aditya.dubey3253#!/vizhome/Trafficanalysisongeomap/Dashboard1)
```angular2
    https://public.tableau.com/profile/aditya.dubey3253#!/vizhome/Trafficanalysisongeomap/Dashboard1
```

### [2.Crash report analysis](https://public.tableau.com/profile/aditya.dubey3253#!/vizhome/crash_16033013839220/Dashboard2)
```angular2
    https://public.tableau.com/profile/aditya.dubey3253#!/vizhome/crash_16033013839220/Dashboard2
```

### [2.City Traffic analysis](https://public.tableau.com/profile/aditya.dubey3253#!/vizhome/Traffic_analysis_16032111787210/Dashboard1)
```angular2
    https://public.tableau.com/profile/aditya.dubey3253#!/vizhome/Traffic_analysis_16032111787210/Dashboard1
```


# Problem
Every city in the world is facing problem of traffic congestion. Instead of investing any furthur resources (more bridges, widen roads, etc), If city can manages our existing resources effeciently, city traffic department can avoid the prolem of traffic congestion. So with the help of traffic data- data driven insights on traffic data is providing to city traffic department to solve the problem.


<b>Motivation</b>  In the city of Chicago there is an one way reversible express lane(speed limit= 70mph) that opens for traffic towards the city on morning and in evening it opens away from the city to traffic.
On Mondays through Fridays, reversible express lanes from the inbound direction to outbound travel between 11:30 a.m. and 1:30 p.m., depending on traffic conditions. Sundays through Fridays, the outbound reversibles are switched to the inbound direction between 11 p.m. and 1 a.m.

This motivated me to provide the data driven insights to city traffic department to use the existing resourses effeciently.


# Architecture
![Pipeline](https://github.com/Ps-budd/Traffic-Bolt/blob/master/Images/Architecture.JPG)

Pipeline Consists of various modules:
1: Amazon S3 
2: Spark
3: Amazon Redshift
4: Tableau

# ETL Flow
1: Data Collected from the API is moved to landing zone s3 buckets.  <br>
2: ETL job has s3 module which copies data from landing zone to working zone- Spark  <br>
3: Once the data is moved to working zone, spark job is triggered which reads the data from S3 and apply transformation and dothe necessary processing.  <br>
4: processed data is put back to s3 buckets.  <br>
5: ETL jobs picks up data from processed zone and stages it into the Redshift staging tables.  <br>
6: Tableau reads data from redshift and shows the dashboards. <br>


# Environment Setup

<b>Spark</b>  
Installation configuration:

    Spark Version: spark-2.4.7-bin-hadoop2.7.tgz
    Java Version: openjdk-8-jre-headless
    Python Version: python3.7.9
    Spark configure:
```    
$ spark-submit --packages com.amazonaws:aws-java-sdk:<version>,org.apache.hadoop:hadoop-aws:2.7.7 --conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true --master local spark_job.py
```



<b>Setting up Redshift</b> You can follow the [AWS Guide](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-prereq.html) to run a Redshift cluster.




# Contact
Feel free to contact [me](https://www.linkedin.com/in/adityadubey09/) or You can email me at adubey6@hawk.iit.edu


