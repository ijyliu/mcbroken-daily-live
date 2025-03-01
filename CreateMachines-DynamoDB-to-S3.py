import boto3
import csv
import io
import json

# Initialize AWS clients
dynamodb = boto3.client('dynamodb')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    # DynamoDB table and S3 bucket
    dynamodb_table = 'McBrokenMachines'
    s3_bucket = 'mcbroken-bucket'
    s3_key = 'updated-mcbroken.csv'

    # Scan DynamoDB table to fetch all data
    response = dynamodb.scan(
        TableName=dynamodb_table
    )

    # Extract the items from DynamoDB response
    items = response.get('Items', [])

    # Check if there is data to process
    if not items:
        return {
            'statusCode': 200,
            'body': json.dumps('No data to process.')
        }

    # Create CSV output in memory
    output = io.StringIO()
    csv_writer = csv.DictWriter(output, fieldnames=items[0].keys())
    
    # Write headers to CSV
    csv_writer.writeheader()

    # Convert DynamoDB items to a format that can be written to CSV
    for item in items:
        row = {key: list(value.values())[0] for key, value in item.items()}
        csv_writer.writerow(row)

    # Reset StringIO pointer to the beginning
    output.seek(0)

    # Upload CSV to S3
    s3.put_object(
        Bucket=s3_bucket,
        Key=s3_key,
        Body=output.getvalue()
    )

    return {
        'statusCode': 200,
        'body': json.dumps(f'Data saved to S3 as {s3_key}')
    }
