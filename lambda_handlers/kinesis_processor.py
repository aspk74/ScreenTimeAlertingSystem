import json
import boto3
import base64
from datetime import datetime

# Initialize clients
dynamodb = boto3.resource('dynamodb')
s3 = boto3.client('s3')

# Your table and bucket name
DYNAMO_TABLE_NAME = 'UserActivity'
S3_BUCKET_NAME = 'screen-time-activity-logs'

def lambda_handler(event, context):
    table = dynamodb.Table(DYNAMO_TABLE_NAME)

    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        data = json.loads(payload)

        # --- Save to DynamoDB ---
        try:
            table.put_item(Item=data)
            print("Saved to DynamoDB:", data)
        except Exception as e:
            print("DynamoDB Error:", e)

        # --- Save to S3 ---
        try:
            timestamp = datetime.utcnow().isoformat()
            s3_key = f"activity_logs/{data['user_id']}/{timestamp}.json"

            s3.put_object(
                Bucket=S3_BUCKET_NAME,
                Key=s3_key,
                Body=json.dumps(data),
                ContentType='application/json'
            )
            print("Saved to S3:", s3_key)
        except Exception as e:
            print("S3 Error:", e)

    return {
        'statusCode': 200,
        'body': json.dumps('Processing complete!')
    }
