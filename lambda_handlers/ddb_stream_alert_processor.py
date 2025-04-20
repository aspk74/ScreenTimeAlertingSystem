import json
import boto3
from datetime import datetime

dynamodb = boto3.resource('dynamodb')
alerts_table = dynamodb.Table('user_alerts')  # Make sure to create this table

def lambda_handler(event, context):
    print("Received event:", json.dumps(event))
    for record in event['Records']:
        if record['eventName'] == 'INSERT':
            new_item = record['dynamodb']['NewImage']

            user_id = new_item['user_id']['S']
            time_spent = int(float((new_item['time_spent']['S'])))

            if time_spent > 1:  # 50 minutes
                alert_text = f"⚠️ High time spent: {time_spent} mins"
                
                timestamp = datetime.utcnow().isoformat()

                # Save the alert to another table
                alerts_table.put_item(
                    Item={
                        'user_id': user_id,
                        'timestamp': timestamp,
                        'alert': alert_text
                    }
                )

                print(f"Alert generated for {user_id}: {alert_text}")
