import json
import boto3

s3_client = boto3.client('s3')
rekognition_client = boto3.client('rekognition')

def lambda_handler(event, context):
    try:
        for record in event['Records']:
            # Extract the message body from the SQS record
            message_body = json.loads(record['body'])
            # Extract bucket name and object key from the message body
            bucket_name = message_body['Records'][0]['s3']['bucket']['name']
            object_key = message_body['Records'][0]['s3']['object']['key']
            
            # Call Rekognition to analyze the image
            response = rekognition_client.detect_labels(
                Image={
                    'S3Object': {
                        'Bucket': bucket_name,
                        'Name': object_key
                    }
                }
            )
            
            # Process Rekognition response
            analyzed_labels = response['Labels']
            # Here you can perform any additional processing on the analyzed labels
            
            # Move the analyzed image to 'folder2' in the same bucket
            destination_key = object_key.replace('folder1', 'folder2')  # Replace 'folder1' with 'folder2' in the object key
            s3_client.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': object_key},
                Key=destination_key
            )
            
            # Delete the original image from 'folder1'
            s3_client.delete_object(Bucket=bucket_name, Key=object_key)
            
            # Optionally, you can log the analyzed labels or any other relevant information
            print("Image analyzed successfully. Labels:", analyzed_labels)
        
        return {
            'statusCode': 200,
            'body': json.dumps('Image analysis complete and moved to folder2')
        }
    except Exception as e:
        # Log the error for debugging purposes
        print(f"Error: {e}")
        raise e  # Re-raise the exception for Lambda to handle
