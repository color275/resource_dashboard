import boto3
import time
import datetime
import pprint
import json
from base64 import encode
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

pi_client = boto3.client('pi')
rds_client = boto3.client('rds')
cw_client = boto3.client('cloudwatch')

host = 'vpc-test-aponilxfo5qn2nfe6mitxf2rxu.ap-northeast-2.es.amazonaws.com' # cluster endpoint, for example: my-test-domain.us-east-1.es.amazonaws.com
region = 'ap-northeast-2'
service = 'es'
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region, service)

es_client = OpenSearch(
    hosts = [{'host': host, 'port': 443}],
    http_auth = auth,
    use_ssl = True,
    verify_certs = True,
    connection_class = RequestsHttpConnection,
    pool_maxsize = 20
)


def get_pi_instances():
    response = rds_client.describe_db_instances()
    
    target_instance = []
    
    for instance in response['DBInstances']:
        for tag in instance.get('TagList', []):
            if tag.get('Key') == 'monitor' and tag.get('Value') == 'true':
                target_instance.append(instance)
                break
    # pprint.pprint(target_instance)
    return target_instance

def get_resource_metrics(instance, query):

    # print(instance['DbiResourceId'])
    # print("instance['DbiResourceId'] : ",instance['DbiResourceId'])
    # print("query : ", query)
    
    return {
                'pi_response': pi_client.get_resource_metrics(
                             ServiceType='RDS',
                             Identifier=instance['DbiResourceId'],
                             StartTime=time.time() - 60,
                             EndTime=time.time(),
                             PeriodInSeconds=60,
                             MetricQueries=query
                             ), 
                'dbinstanceidentifier': instance['DBInstanceIdentifier']
            }

def remove_non_ascii(string):
    non_ascii = ascii(string)
    return non_ascii

def str_encode(string):
    encoded_str = string.encode("ascii","ignore")
    return remove_non_ascii(encoded_str.decode())

def send_cloudwatch_data(get_info):
    
    metric_data = []
    
    pprint.pprint(get_info['pi_response']['MetricList'])
    for metric_response in get_info['pi_response']['MetricList']: #dataoints and key
        metric_dict = metric_response['Key']  #db.load.avg
        metric_name = metric_dict['Metric']

     
        is_metric_dimensions = False
        formatted_dims = []
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']  # return a dictionary
            
            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key])))
                
                # if key == "db.sql_tokenized.statement":
                #     formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key])))
                # else:
                #     formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key]))) 

            formatted_dims.append(dict(Name='DBInstanceIdentifier', Value=get_info['dbinstanceidentifier']))
            is_metric_dimensions = True
        else:
            pass
            # metric_name = metric_name.replace("avg","")

        for datapoint in metric_response['DataPoints']:
            # We don't always have values from an instance
            value = datapoint.get('Value', None)
            if value:
                if is_metric_dimensions:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': formatted_dims,
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    })
                else:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': [
                            {
                                'Name':'DBInstanceIdentifier',    
                                'Value':get_info['dbinstanceidentifier']
                            } 
                        ],
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    }) 
    
    if metric_data:
        # logger.info('## sending data to cloduwatch...')
        try:
            cw_client.put_metric_data(
            Namespace= 'PI-TEST3',
            MetricData= metric_data)
        except ClientError as error:
            raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    else:
        # logger.info('## NO Metric Data ##')
        pass

def send_opensearch_data(get_info):
    metric_data = []
    
    pprint.pprint(get_info['pi_response']['MetricList'])
    for metric_response in get_info['pi_response']['MetricList']:
        metric_dict = metric_response['Key']
        metric_name = metric_dict['Metric']

        is_metric_dimensions = False
        formatted_dims = []
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']
            
            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append({'Name': key, 'Value': str_encode(metric_dimensions[key])})

            formatted_dims.append({'Name': 'DBInstanceIdentifier', 'Value': get_info['dbinstanceidentifier']})
            is_metric_dimensions = True

        for datapoint in metric_response['DataPoints']:
            value = datapoint.get('Value', None)
            if value:
                if is_metric_dimensions:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': formatted_dims,
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    })
                else:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': [
                            {
                                'Name': 'DBInstanceIdentifier',
                                'Value': get_info['dbinstanceidentifier']
                            }
                        ],
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    }) 
    
    if metric_data:
        # try:
        for metric in metric_data:
            document = {
                'timestamp': metric['Timestamp'].isoformat(),
                'metric_name': metric['MetricName'],
                'value': metric['Value']
            }
            if metric['Dimensions']:
                for dim in metric['Dimensions']:
                    document[dim['Name']] = dim['Value']
            
            es_client.index(
                index='test_pi_metric',
                body=document
            )
        # except Exception as error:
        #     raise ValueError('Failed to send data to OpenSearch: {}'.format(error))
    else:
        pass

# def send_cloudwatch_data(get_info):
#     metric_data = []

#     # print(get_info['dbinstanceidentifier'])

#     # pprint.pprint(get_info['pi_response'])

#     pprint.pprint(get_info['pi_response'])
#     for metric_response in get_info['pi_response']['MetricList']:
#         # pprint.pprint(pi_response)
#         cur_key = metric_response['Key']['Metric']

#         for datapoint in metric_response['DataPoints']:
#             # We don't always have values from an instance
#             value = datapoint.get('Value', None)

#             if value:
#                 metric_data.append({
#                     'MetricName': cur_key,
#                     'Dimensions': [
#                         {
#                             'Name':'DBInstanceIdentifier',    
#                             'Value':get_info['dbinstanceidentifier']
#                         } 
#                     ],
#                     'Timestamp': datapoint['Timestamp'],
#                     'Value': datapoint['Value']
#                 })

#     if metric_data:
#         # pprint.pprint(metric_data)
#         cw_client.put_metric_data(
#             Namespace='PI-TEST',
#             MetricData= metric_data
#         )



pi_instances = get_pi_instances()

file_path = "./query.json"
with open(file_path, 'r') as file:
    metric_queries = json.load(file)

print(len(metric_queries))

# 구성원 배열: 최소 항목 수: 1개. 최대 15개 항목.
# https://docs.aws.amazon.com/ko_kr/performance-insights/latest/APIReference/API_GetResourceMetrics.html#API_GetResourceMetrics_RequestSyntax
limit_query_num = 15

querys = []

for i in range(0, len(metric_queries), limit_query_num):
    query = metric_queries[i:i+limit_query_num]
    querys.append(query)

for query in querys :

    for instance in pi_instances:
            get_info = get_resource_metrics(instance, query)
            if get_info['pi_response']:
                # send_cloudwatch_data(get_info)
                send_opensearch_data(get_info)
            
    print("# Put Data to CW : ", datetime.datetime.now())


