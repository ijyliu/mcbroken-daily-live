import json
import urllib.request
import boto3
from datetime import datetime

# Initialize DynamoDB client
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('McBrokenMachines')

# URL of the JSON file
url = "https://raw.githubusercontent.com/rashiq/mcbroken-archive/refs/heads/main/mcbroken.json"

def lambda_handler(event, context):

    # Fetch the JSON data from the URL
    response = urllib.request.urlopen(url)
    data = json.load(response)
    
    # Initialize counters
    total_machines = 0
    broken_machines = 0
    
    # Loop through the data and count
    for entry in data:
        total_machines += 1
        if entry['properties']['is_broken']:
            broken_machines += 1
    
    # Get the current date for filename
    current_date = datetime.now().strftime('%Y-%m-%d')

    # Batch write data to DynamoDB
    with table.batch_writer() as batch:

        # Put item into DynamoDB
        batch.put_item(
            Item={
                "date": current_date,
                "datetime": datetime.now().isoformat(),
                "total_machines": total_machines,
                "broken_machines": broken_machines
            }
        )

    return {
        'statusCode': 200,
        'body': json.dumps(f"Data for {current_date} saved to DynamoDB")
    }
