# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType,StructField,IntegerType
import pyspark.sql.functions as F
import json

##Write into postgres
def write_to_postgres(df, epoch_id):
    database_url = dbutils.secrets.get('usecaseScope','database_url')
    postgres_table = "info"
    properties = {
    "user": dbutils.secrets.get('usecaseScope','user'),
    "password": dbutils.secrets.get('usecaseScope','password'),
    "driver": "org.postgresql.Driver"}
    df.write.jdbc(url=database_url, table=postgres_table, mode="append", properties=properties)

##Explode
def exploded_df(df):
    ex_df =   df.select('body.*')
    ex_df1 =   ex_df.select('gender',"name.title","location.*","email","login.*","dob.*",F.col("registered.date").alias("registered_date"),"phone","cell","id.*","picture.*","nat")
    return ex_df1

##DQ Checks
def data_quality(df):
    dq_df1=df.dropna(subset="name") #if name is null remove row
    dq_df2=dq_df1.dropDuplicates()   #removing duplicates 
    filtered_df = dq_df2.filter(F.col('email').contains("@")) #considering records which'@' symbol in email fields 
    return filtered_df

##Transform
def transform(df):
    ts_df = df.withColumn("full_name", F.split(df["email"], '@').getItem(0))
    ts_df = ts_df.select("title","full_name","email")
    return ts_df


json_schema=StructType([
        StructField("gender",StringType(),True),
        StructField("name",StructType([StructField("title",StringType(),True),StructField("first",StringType(),True),StructField("last",StringType(),True)]),True),
        StructField("location",StructType([StructField("city",StringType(),True),StructField("state",StringType(),True),StructField("country",StringType(),True)]),True),
        StructField("email",StringType(),True),
        StructField("login",StructType([StructField("username",StringType(),True),StructField("password",StringType(),True)]),True),
        StructField("dob",StructType([StructField("date",StringType(),True),StructField("age",IntegerType(),True)]),True),
        StructField("registered",StructType([StructField("date",StringType(),True),StructField("age",IntegerType(),True)]),True),
        StructField("phone",StringType(),True),
        StructField("cell",StringType(),True),
        StructField("id",StructType([StructField("name",StringType(),True),StructField("value",StringType(),True)]),True),
        StructField("picture",StructType([StructField("large",StringType(),True),StructField("thumbnail",StringType(),True)]),True),
        StructField("nat",StringType(),True)
    ])

connectionString = " "

startingEventPosition={
    "offset": "-1",
    "seqNo": -1,
    "enqueuedTime": None,
    "isInclusive": True
 }
ehConf = {}
ehConf["eventhubs.connectionString"] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)
ehConf["eventhubs.consumerGroup"] = "$Default"  
ehConf["eventhubs.startingPosition"] = json.dumps(startingEventPosition)
ehConf["eventhubs.maxEventsPerTrigger"]= 5

try:
    #SparkSession
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("UseCase").getOrCreate()
    ##Read Stream
    df= spark.readStream.format("eventhubs").options(**ehConf).load()
    df1 = df.withColumn("body", F.from_json(df.body.cast("string"), json_schema))

    ##Calling appropritate functions i.e exploded,data_quality,transform
    ex_df = exploded_df(df1)
    dq_df = data_quality(ex_df)
    ts_df = transform(dq_df)
    display(ts_df)
    
    ## calling write_to_postgres to write df into Postgres 
    query = ts_df.writeStream.foreachBatch(write_to_postgres).outputMode("append").start()

except Exception as e:
    raise Exception(e)
