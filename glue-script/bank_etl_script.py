import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col
from awsglue.dynamicframe import DynamicFrame

# Get Job Arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

# Create Spark & Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Initialize Job
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# ============================
# STEP 1: Read Raw CSV from S3
# ============================

df = spark.read \
    .option("header", "true") \
    .option("delimiter", ";") \
    .csv("s3://awsglueetlbucket/banking-raw-data/")

# ============================
# STEP 2: Transform / Clean Data
# ============================

df = df.withColumn("age", col("age").cast("int"))
df = df.withColumn("balance", col("balance").cast("int"))

df = df.drop(
    'contact',
    'day',
    'month',
    'duration',
    'pdays',
    'previous',
    'poutcome',
    'y',
    'campaign'
)

df = df.filter(col("education") != "unknown")
df = df.filter(col("job") != "unknown")

df = df.dropna()

# ============================
# STEP 3: Convert to DynamicFrame
# ============================

dynamic_frame = DynamicFrame.fromDF(
    df,
    glueContext,
    "dynamic_frame"
)

# ============================
# STEP 4: Write to S3 as Parquet
# ============================

s3_output_path = "s3://awsglueetlbucket/banking-processed-data/"

s3_sink = glueContext.getSink(
    path=s3_output_path,
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=False,   # IMPORTANT: Avoid catalog error
    transformation_ctx="s3_sink"
)

# Set Parquet Format
s3_sink.setFormat("glueparquet")

# Write Data
s3_sink.writeFrame(dynamic_frame)

# ============================
# STEP 5: Commit Job
# ============================

job.commit()
