from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import *
import pyspark.sql.functions as f
from pyspark.sql.functions import unix_timestamp, from_unixtime, date_format
import configparser
import org.apache.spark.sql.functions._

 #read csv from s3 to spark
def read_s3_csv(key):
    s3file = f's3a://{bucket}/{key}'
    df = spark.read.option("header",True).csv(s3file)
    return df

# key and bucket name

config = configparser.ConfigParser()
config.read("s3_properties.ini")
s3_prop = config['s3']
bucket = s3_prop['bucket']    
traffic = 'trafic_data*.csv'
crash = '*incidents.csv'
signal_list = '*signal_listing.csv'
weather = '*weather.csv'


# spark configuration set up
sc = SparkContext()
sc._jsc.hadoopConfiguration().set('fs.s3a.endpoint', 's3.amazonaws.com')

spark = SparkSession(sc)


# Traffic dataset input
df_traffic = read_s3_csv(traffic) \
    .select(
         'Time'      #timestamp need to be saperated
        , 'segment_id'
        , 'segment_speed'
        , 'Direction'
        , 'from'
        , 'to'
        , 'length'
        , 'vehicle_count'
        , 'record_id'
        , 'start_lat'
        , 'start_lon'
        , 'end_lat'
        , 'end_lon'
        )

# Traffic dataset input
df_crash = read_s3_csv(incidents) \
    .select(
         'crash_record'      
        , 'crash_date'        #timestamp need to be saperated
        , 'posted_speed'
        , 'type'
        , 'street_no'
        , 'latitude'
        , 'longitude'
        )    

# weather_dataset input
df_weather = read_s3_csv(weather) \
    .select(
         'date_time'      #timestamp need to be saperated
        , 'Max-tem'
        , 'min-tem'
        , 'wind'
        , 'visibility'
        , 'condition'
  
        )        

# weather_dataset input
df_signal_list = read_s3_csv(signal_listing) \
    .select(
         'Intersection'     
        , 'lon'
        , 'lat'
   
        )   

#cleaning the datasets 

df_traffic =df_traffic.withColumn("date", func.to_date(func.col("Time")))

df_crash =df_crash.withColumn("date", func.to_date(func.col("crash_date")))

df_weather=df_weather.withColumn("date", func.to_date(func.col("date_time)))


#traffic speed normalisation
#counting no of negative values
df_traffic.filter(func.col("speed") <= 0).count()

traffic_speed_avg= df_traffic.select(mean(df("speed")))


# replacing negative speed with average speed
for i in df_traffic.iloc[:,df_traffic.columns.get_loc("speed")]<0:
for index, j in enumerate(df_traffic[i]):
    if j<0:
        df_traffic.at[index, i] = traffic_speed_avg



#crash record cleaning

df_crash.filter(func.col("posted_speed") <= 10).count().show()

crash_speed_avg= df_crash.select(mean(df("posted_speed")))

#traffic_speed_avg.show()


# replacing negative speed with average speed
for i in df_traffic.iloc[:,df_traffic.columns.get_loc("speed")]<10:
for index, j in enumerate(df_traffic[i]):
    if j<0:
        df_crash.at[index, i] = crash_speed_avg




# removing row with missing segment id
df_traffic = df_traffic.filter((df_traffic.segment != 'NULL') 


# connect traffic  datasets with crashes datasets by inner join

df_traffic = df_traffic \
    .join(df_crash
          , df_traffic['start_lon'] == df_crash['lon']) & df_traffic['start_lat'] == df_crash['lat'])\
    .withColumn('row_num'
                , row_number().over(Window.partitionBy(
                                            'date'                                                     ) \
                                          .orderBy('segment_id'))) \
    .select(
        'Time'      #timestamp need to be saperated
        , 'segment_id'
        , 'segment_speed'
        , 'Direction'
        , 'from'
        , 'to'
        , 'length'
        , 'vehicle_count'
        , 'record_id'
        , 'start_lat'
        , 'start_lon'
        , 'end_lat'
        , 'end_lon'
        ,df_crash.crash_record     
        , df_crash.crash_date        
        , df_crash.posted_speed
        , df_crash.type
        , df_crash.street_no
        ) \
   


# df_traffic.show()


#writie data frame to s3

#val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load("s3a://test_system/transcation.csv")
    
#writing to s3 bucket as raw data .Here transcation.csv file.
df_traffic.write.format("csv").option("header","true").mode("Overwrite")
     .save("s3a://S3 file path")
     }

df_weather.write.format("csv").option("header","true").mode("Overwrite")
     .save("s3a://S3 file path")



spark.stop()



