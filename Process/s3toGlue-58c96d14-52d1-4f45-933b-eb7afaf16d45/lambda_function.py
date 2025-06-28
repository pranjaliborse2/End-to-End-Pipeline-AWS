import boto3
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue = boto3.client('glue')

def lambda_handler(event, context):
    crawler_name = "nfip-policies-parquet-catalog"

    try:
        response = glue.start_crawler(Name=crawler_name)
        logger.info(f"Crawler '{crawler_name}' started successfully.")
        return {
            'statusCode': 200,
            'body': f"Crawler '{crawler_name}' started."
        }
    except glue.exceptions.CrawlerRunningException:
        logger.warning(f"Crawler '{crawler_name}' is already running.")
        return {
            'statusCode': 200,
            'body': f"Crawler '{crawler_name}' is already running."
        }
    except Exception as e:
        logger.error(f"Error starting crawler: {str(e)}")
        return {
            'statusCode': 500,
            'body': f"Error: {str(e)}"
        }