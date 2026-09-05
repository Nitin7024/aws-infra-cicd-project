import json
import boto3
import os
from datetime import datetime

# Initialize AWS clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')

def lambda_handler(event, context):
    """
    Lambda function triggered by S3 upload events
    Processes the uploaded file and sends SNS notification
    """
    
    print(f"Received event: {json.dumps(event)}")
    
    try:
        # Get SNS Topic ARN from environment variable
        sns_topic_arn = os.environ.get('SNS_TOPIC_ARN')
        
        # Process each S3 record
        for record in event['Records']:
            # Extract S3 event details
            bucket_name = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            event_time = record['eventTime']
            file_size = record['s3']['object']['size']
            
            print(f"Processing file: {object_key} from bucket: {bucket_name}")
            
            # Get object metadata
            response = s3_client.head_object(
                Bucket=bucket_name,
                Key=object_key
            )
            
            content_type = response.get('ContentType', 'unknown')
            last_modified = response.get('LastModified', 'unknown')
            
            # Prepare notification message
            message = f"""
Document Upload Notification
============================

File Details:
- File Name: {object_key}
- Bucket: {bucket_name}
- Size: {file_size} bytes
- Content Type: {content_type}
- Upload Time: {event_time}
- Last Modified: {last_modified}

Processing Status: SUCCESS
Environment: {os.environ.get('ENVIRONMENT', 'unknown')}
Processed At: {datetime.now().isoformat()}
"""
            
            # Send SNS notification
            if sns_topic_arn:
                sns_response = sns_client.publish(
                    TopicArn=sns_topic_arn,
                    Subject=f"Document Processed: {object_key}",
                    Message=message
                )
                print(f"SNS notification sent: {sns_response['MessageId']}")
            else:
                print("WARNING: SNS_TOPIC_ARN not configured")
            
            # Log success
            print(f"Successfully processed: {object_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Document processed successfully',
                'filesProcessed': len(event['Records'])
            })
        }
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        
        # Send error notification
        if sns_topic_arn:
            error_message = f"""
Document Processing Error
=========================

Error: {str(e)}
Event: {json.dumps(event)}
Time: {datetime.now().isoformat()}
"""
            sns_client.publish(
                TopicArn=sns_topic_arn,
                Subject="ERROR: Document Processing Failed",
                Message=error_message
            )
        
        raise e
