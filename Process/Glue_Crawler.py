# Define the function to start Glue Crawler

import boto3

def start_glue_crawler(crawler_name):
    client = boto3.client('glue', region_name='us-east-1')
    response = client.start_crawler(Name=crawler_name)
    return response