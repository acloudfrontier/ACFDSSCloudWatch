# Code for custom code recipe acf_amazon_cloud_watch (imported from a Python recipe) 
# J.L.G.

# import the classes for accessing DSS objects from the recipe
import dataiku
# Import the helpers for custom recipes
from dataiku.customrecipe import *

# Output dataset, containing the token retrieved from last API call to Amazon CloudWatch:
output_feedbacks = get_output_names_for_role('feedback_output')
feedback_output_dataset = [dataiku.Dataset(name) for name in output_feedbacks]

# The configuration consists of the parameters set up by the user in the recipe Settings tab.
# Retrieve parameter values from the of map of parameters
access_key = get_recipe_config()['access_key']
secret_key = get_recipe_config()['secret_key']

##############################################
# Recipe
##############################################
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
import boto3
import datetime
import ast

# Initialization of boto3's cloudwatch object to push data to Amazon CloudWatch
cloudwatch = boto3.client('cloudwatch', 
                          region_name='eu-west-1', # Change the Amazon CloudWatch Location when required.
                          aws_access_key_id=access_key,
                          aws_secret_access_key=secret_key)

# Class to process rows that need to be sent to CloudWatch
class row_content:
    namespace = ""
    timestamp = ""
    metric_name = ""
    value = ""
    dimensions = ""

##############################################
# Function to put metrics to CloudWatch 
##############################################
def put_metric_to_cloudwatch(row_data):
    response = cloudwatch.put_metric_data(
        Namespace = row_data.namespace,
        MetricData = [
            {
                'Timestamp': row_data.timestamp,
                'MetricName': row_data.metric_name,
                'Dimensions': row_data.dimensions,
                'Value': row_data.value,
                'Unit': 'None'
            },
        ]
    )

##############################################
# Function to get token value 
##############################################
def get_token(dataset_df, key):
    if dataset_df == None:
        return None
    
    key_condition = (dataset_df['key'] == key)
    dataset_row = dataset_df[key_condition] 
    if len(dataset_row.value) == 1:
        token_value = dataset_row.value.tolist()[0]
    else:
        token_value = -1
    return token_value

##############################################
# Function to process data 
##############################################
def process_dataset (dataset_df, token_value):
    if token_value is not None:
        filter_cond = (dataset_df['Timestamp'] >= token_value)
        procesed_dataset_df = dataset_df[filter_cond]
    else:
        procesed_dataset_df = dataset_df
    print('Processing columns: %s' % list(dataset_df.columns))
    count = 0
    errors = 0
    row_data = row_content()    
    for index, row in procesed_dataset_df.iterrows():
        # Manipulate dataset here, and call the put_metric_to_cloudwatch function for each row.
        row_data.namespace = row.Namespace
        row_data.timestamp = row.Timestamp
        row_data.metric_name = row.MetricName
        row_data.value = row.MetricValue
        try:
            row_data.dimensions = ast.literal_eval(row.MetricDimensions)
        except:
            row_data.dimensions = []
        try:
            put_metric_to_cloudwatch(row_data)
            count += 1
        except Exception as ex:
            print(ex)
    dataset_new_token = datetime.datetime.now().isoformat()
    print('[ACF_CLOUDWATCH plugin] - ' + str(count) + ' rows successfully processed. Token value: ' + dataset_new_token)
    print('[ACF_CLOUDWATCH plugin] - %d errors' % errors)
    return (dataset_new_token, count, errors)

#############################################################
# Function to copnvert all non-standard columns as dimensions
#############################################################
def build_dimensions(indsdf):
    # Basic columns. Value and Unite prefixed with Metric
    metricColumns = \
        ["Namespace","MetricName","Timestamp","MetricValue","MetricUnit"]
    metricDimensions = \
        indsdf[[c for c in  indsdf.columns if not c in metricColumns]]
    print('metricColumns: %s' % metricColumns)
    print('metricDimensions: %s' % list(metricDimensions.columns))
    # Dimensions generator
    def process_dimensions(row):
        arr = [json.dumps({"Name": c, "Value": str(row[c])}) \
                for ix,c \
                in enumerate(metricDimensions.columns) if str(row[c]) != "nan"]
        dms = "[%s]" % ",".join(arr)
        return dms

    indsdf["MetricDimensions"] = indsdf.apply(process_dimensions, axis=1)
    
    # Drop dimension columns
    indsdf = indsdf.drop(metricDimensions.columns, axis=1)
    return indsdf

def load_feedback_dataset():
    try:
        input_feedback = get_output_names_for_role('feedback_output')
        if len(input_feedback) != 0:
            feedback = dataiku.Dataset(input_feedback[0])
            return feedback.get_dataframe()
    except:
        pass
    return None

#############################################################
# Function to iterate over all inputs
#############################################################
def process_inputs():
    # Get feedback
    feedback_df = load_feedback_dataset()
        
    names = get_input_names_for_role('metrics')
    for name in names:
        process_input(name, feedback_df)
    # Write the output to the output dataset
    feedback_output_dataset[0].write_with_schema(pd.DataFrame(output_feedback))

    
def process_input(dsname, feedback_df):
    print('Processing dataset: %s' % dsname)
    df = dataiku.Dataset(dsname).get_dataframe()
    df = build_dimensions(df)
    token_value = get_token(feedback_df, dsname)
    token_value, count, errors = process_dataset(df, token_value)
    output_feedback.append({ "key": dsname, "value": token_value, "count": count, "errors": errors })
    print('%s input processed correctly.' % dsname)


##############################################
# Main
##############################################
output_feedback = []

process_inputs()
