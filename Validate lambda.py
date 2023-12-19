import os
import json
import boto3
import io





def lambda_handler(event, context):
    #read the payload used to trigger the lambda function through eventbrdge
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    folder_name = event['Records'][0]['s3']['folder_name']['key']

    ##bucket_name = 'MR'
    ##folder_name = 'unrefined/Loc111/'


    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(bucket_name)
        try:
            For obj in bucket.objects.filter(Prefix=folder_name):
                print('Object to extract :', obj)
                print('obj key: ', obj.key)
                s3_client = boto3.client('s3')
                json_obj = s3_client.get_object(Bucket=bucket_name, Key=obj.key)
                json_data = json_obj["Body"].read().decode('utf-8')
                read_json_files.append(files)
                if not json_data:
                    print("Skipping empty", obj.key)
                    continue
                json_dict = json.loads(json_data)

        except ValueError as e:
                print('invalid json file:' % e)
                return None