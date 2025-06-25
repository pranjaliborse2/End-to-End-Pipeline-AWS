import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import gs_null_rows

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1750365625035 = glueContext.create_dynamic_frame.from_catalog(database="nfip-policies-db", table_name="policies_by_state", transformation_ctx="AWSGlueDataCatalog_node1750365625035")

# Get a list of fields with multiple types
conflicts = AWSGlueDataCatalog_node1750365625035.schema().fields
to_resolve = [
    (field.name, "cast:string")
    for field in conflicts
    if "ChoiceType" in str(field.dataType)
]
# Apply resolution
dyf = AWSGlueDataCatalog_node1750365625035.resolveChoice(specs=to_resolve)

# Script generated for node Remove Null Rows
RemoveNullRows_node1750366716730 = dyf.gs_null_rows()

# Script generated for node Amazon S3
AmazonS3_node1750365507594 = glueContext.write_dynamic_frame.from_options(frame=RemoveNullRows_node1750366716730, connection_type="s3", format="glueparquet", connection_options={"path": "s3://nfip-policies-transformed-parquet/policies-by-state/", "partitionKeys": ["region"]}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1750365507594")

job.commit()