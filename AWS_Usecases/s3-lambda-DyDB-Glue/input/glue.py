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
meta_path = "s3://my-uc-bkt/meta/"
args = getResolvedOptions(sys.argv, ["bkt","file","target_file_type","target_path"])
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
    target_path = args['target_path']
    filename = args['file'].split("/")[-1].split(".")[0]
    output_path = f"{target_path}tables/{frame}"
    print("path", output_path)
    glueContext.write_dynamic_frame.from_options(frame = df.coalesce(1),
                                                 connection_type = "s3",
                                                 connection_options = {"path": output_path},
                                                 format = args['target_file_type'])
