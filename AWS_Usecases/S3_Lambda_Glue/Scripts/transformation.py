from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import *
from pyspark.sql import SparkSession
import sys

# Initialize Spark and Glue contexts
spark = SparkSession.builder.appName("Glue").getOrCreate()
glueContext = GlueContext(spark.sparkContext)


# @params: [JOB_NAME]
meta_path = "s3://my-uc-meta-bkt/meta/"
args = getResolvedOptions(sys.argv, ["bkt","file"])
input_path = f"s3://{args['bkt']}/{args['file']}"

if input_path.endswith(".json"):
    # Read the JSON file from S3
    datasource0 = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [input_path]},
        format="json"
    )
if input_path.endswith(".xml"):
    datasource0 = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [input_path]},
        format="xml",
        format_options={
            "rowTag": "root"
        }
    )

#Apply Transformation
relationalized_datasource = datasource0.relationalize("root", meta_path)



for frame in relationalized_datasource.keys():
    df = relationalized_datasource.select(frame)
    output_path = f"s3://my-uc-bkt/output/tables/{frame}"
    print("path", output_path)
    glueContext.write_dynamic_frame.from_options(frame = df.coalesce(1),
                                                 connection_type = "s3",
                                                 connection_options = {"path": output_path},
                                                 format = "parquet")

# for k in relationalized_datasource.keys():
#     print(k)
#     relationalized_datasource.select(k).toDF().show()
    

#Dynamically join all the df's (which are in relationalized_datasource)   
# joined_df = relationalized_datasource['root']
# for k in relationalized_datasource.keys():
#     if k not in ("root"):
#         print(k)   
#         parent_key = k.split("_")[1]
#         print(parent_key)
#         joined_df = Join.apply(
#             frame1=joined_df,
#             keys1=[parent_key],
#             frame2=relationalized_datasource[k],
#             keys2=['id']
#         )

# # joined_df.toDF().show()


# glueContext.write_dynamic_frame.from_options(
#     frame=joined_df.coalesce(1),
#     connection_type="s3",
#     connection_options={"path": output_path,"filename": "example.json"},
#     format="parquet"
# )
