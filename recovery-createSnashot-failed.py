import json
import boto3
import time
import os
import secrets

# Requirements
# Set the Lambda timeout to at least 1 minute
#
# Define the following environment variables for this Lambda:
# - the lambda function scope is limited to those snapshot with source_tag_Key=source_tag_Value 
#   >>source_tag_Key
#   >>source_tag_Value
# - the lambda function will append the following tags
#   >>snapshot_recovery_tag_Key
#   >>snapshot_recovery_tag_Value
#
# Example - By setting the following Environment Variable, only EBS Snapshot with tag EBS-Snapshot=LambdaRecovery 
# will be managed by this lambda which will append Retention=30
# The following tag values are just an example, you have to set the values accordingly your needs
#
# | ----------------------------------- | ------------------ |
# | Tag Key                             | Tag Value Example  |
# | ----------------------------------- | ------------------ |
# | snapshot_recovery_tag_Key	        | Retention          |
# | snapshot_recovery_tag_Value         | 30                 | 
# | source_tag_Key	                    | EBS-Snapshot       |
# | source_tag_Value                    | LambdaRecovery     |
# | snapshot_recovery_max_retries_sns   | ARN-SNS-TOPIC      | 
# | ----------------------------------- | ------------------ |

def lambda_handler(event, context):
    for message in event['Records']:
        process_message(message)

def process_message(message):
    batch_item_failures = []
    sqs_batch_response = {}
    try:
        client = boto3.client('ec2')
        resource = boto3.resource('ec2')
        body=message['body']
        json_dict = json.loads(body)
        detail=json_dict["detail"]
        snapshotId=detail['snapshot_id'].split("/")[1] 
        snapshot=resource.Snapshot(snapshotId)
        snapshot_tags=snapshot.tags
        # Tag needed on the failed snapshot to proceed with this lambda
        source_tag={'Key': os.environ['source_tag_Key'], 'Value': os.environ['source_tag_Value']} # Tags to identify failed snapshots to be recovered
        print("BEFORE - Source Tag ",source_tag)
        if source_tag in snapshot_tags:
            # print("Snapshot tags: ", source_tag, " are in snapshot_tags ", snapshot_tags,"--",type(snapshot_tags))
            # Tag to identify EBS snapshots done by this lambda
            snapshot_recovery_tag={'Key': os.environ['snapshot_recovery_tag_Key'], 'Value': os.environ['snapshot_recovery_tag_Value']}
            if snapshot_recovery_tag in snapshot_tags: # True if the snapshot has previously been recovered but it failed again
                # print("Snapshot has been previously recovered -> snapshot_tags=",snapshot_tags)
                value=next(item["Value"] for item in snapshot_tags if item["Key"] == "SnapshotRecoveryCounter")
                if value=='0':
                    print("Max attempts reached -> snapshot will not be retried")
                    VolumeId=detail['source'].split("/")[1] 
                    sns_message = "Lambda Function "+ os.environ['AWS_LAMBDA_FUNCTION_NAME'] + "("+os.environ['AWS_REGION']+") was not able to make an EBS Snapshot for VolumeID "+ VolumeId + "\n Please check it."
                    sns_client = boto3.client('sns')
                    response = sns_client.publish(TopicArn=os.environ['snapshot_recovery_max_retries_sns'],Message=sns_message)
                    print("Max attempts reached -> Message published ", response)
                    return
                else:
                    print("Snapshot recovery attempt failed -> current snapshot_tags=",snapshot_tags)
                    for item in snapshot_tags:
                        if item['Key'] == 'SnapshotRecoveryCounter':
                            item['Value'] = str(int(value)-1)
                    print("Snapshot recovery attempt failed -> counter decreased by 1 -> snapshot_tags=",snapshot_tags)
            else:
                print("Snapshot first recovery attempt -> Appending snapshot_tags=",snapshot_tags)
                tagcounter={'Key': 'SnapshotRecoveryCounter', 'Value': '5'} # Tag to count retries on this volume
                snapshot_tags.append(tagcounter)
                snapshot_tags.append(snapshot_recovery_tag)
            # To avoid SnapshotCreationPerVolumeRateExceeded we have to wait 15 seconds
            sleed_delay = 15 + secrets.randbelow(60)
            print("Sleeping for ", sleed_delay, "sec.")
            time.sleep(sleed_delay)
            VolumeId=detail['source'].split("/")[1]
            response = client.create_snapshot(
                Description='Snapshot for {} created by Lambda to overcome an EBS snapshot failed'.format(VolumeId),
                VolumeId=VolumeId,
                TagSpecifications=[{'ResourceType': 'snapshot', 'Tags':snapshot_tags}])
            print("Response:",response)
                
    except Exception as e:
        print("Exception ",e)
        batch_item_failures.append({"itemIdentifier": message['messageId'], "VolumeID": VolumeId, "Region": body['region']})
        
    sqs_batch_response["batchItemFailures"] = batch_item_failures
    return sqs_batch_response