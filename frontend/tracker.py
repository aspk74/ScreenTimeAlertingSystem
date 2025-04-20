import time
import boto3
import json

def get_mock_activity(start_time):
    return {
        "user_id": "func-test-1",
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
        "activity": "Netflix",
        "category": "Entertainment",
        "time_spent": str(round((time.time()-start_time)/60,2))
    }

kinesis = boto3.client('kinesis', region_name='us-east-2')
start_time = time.time()
while True:
    data = get_mock_activity(start_time)
    response = kinesis.put_record(
        StreamName='ActivityStream',
        Data=json.dumps(data),
        PartitionKey=data['user_id']
    )
    print(f"response: {response}")
    print(f"Sent activity: {data}")
    time.sleep(60)  # every minute
