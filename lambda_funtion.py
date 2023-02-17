import json
import os
import uuid

from datetime import datetime
from decimal import Decimal
from enum import IntEnum

import boto3
import pandas as pd

from pyarrow.parquet import ParquetDataset

from boto3.dynamodb.conditions import Key
from scrappy import WebScraper

MODEL_OUTPUT_S3_PREFIX = os.environ.get("MODEL_OUTPUT_S3_PREFIX")
OFFLOAD_S3_BUCKET = os.environ.get("OFFLOAD_S3_BUCKET")
OFFLOAD_S3_PREFIX = os.environ.get("OFFLOAD_S3_PREFIX")
PER_SOURCE_RESULTS = int(os.environ.get("PER_SOURCE_RESULTS", 5))

S3_CLIENT = boto3.client("s3")

class Action(IntEnum):
    GENERATE = 1
    TEST = 2

def put_content(url, keywords, training_need_id, interest_area_id, competence_id, source, title, metadata, raw):
    dynamodb = boto3.resource('dynamodb')

    table = dynamodb.Table('content-repository')
    response = table.put_item(
       Item={
            'id': str(uuid.uuid4()),
            'url': url,
            'keywords': keywords,
            'training_need_id': training_need_id,
            'interest_area_id': interest_area_id,
            'competence_id': competence_id,
            'source': source,
            'title': title,
            'metadata': metadata,
            'raw': json.loads(json.dumps(raw), parse_float=Decimal),
            'datetime': str(datetime.utcnow().isoformat())
        }
    )
    # print(response)
    return response

def get_content(url, training_need_id, interest_area_id, competence_id):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('content-repository')
    
    result = table.scan(
        FilterExpression=Key('url').eq(url) & Key('training_need_id').eq(training_need_id) & Key('interest_area_id').eq(interest_area_id) & Key('competence_id').eq(competence_id)
    )
    return result.get('Items', [])

def read_table(table_name):
    schema = table_name.split(".")[0]
    name = table_name.split(".")[1]
    

    dataset = ParquetDataset('s3://mangus-datascience-data/db/mangus/training_needs/dump.parquet')
    table = dataset.read()
    df = table.to_pandas()
    print(df)
    return df


def generate():

    dict_frame = {}

    dict_frame["df_training_needs"] = read_table("mangus.training_needs")
    dict_frame["df_competences"] = read_table("mangus.competences")
    dict_frame["df_interest_areas"] = read_table("mangus.interest_areas")

    scrappy_result = dict()

    

def test(meta_type, meta_id):
    df = pd.read_json(f"s3://{OFFLOAD_S3_BUCKET}/{MODEL_OUTPUT_S3_PREFIX}/links.json")
    #result_by_id_df = df.loc[df[meta_type] == meta_id]
    final_result = df.to_dict('records')
    
    for l in final_result:
        for g in l["google"]:
            result = get_content(g["link"], l["training_need_id"], l["interest_area_id"], l["competence_id"])
            if not result:
                print(f"{g['link']} - {result}")
                put_content(g["link"], l["keywords"], l["training_need_id"], l["interest_area_id"], l["competence_id"], "google", g["title"], g["metadata"], g)
        for y in l["youtube"]:
            result = get_content(y["link"], l["training_need_id"], l["interest_area_id"], l["competence_id"])
            if not result:
                print(f"{y['link']} - {result}")
                put_content(y["link"], l["keywords"], l["training_need_id"], l["interest_area_id"], l["competence_id"], "youtube", y["title"], y["metadata"], y)
    
    #final_result = {
    #    "youtube": result_by_id_df["youtube"].iloc[0][:PER_SOURCE_RESULTS],
    #    "google": result_by_id_df["google"].iloc[0][:PER_SOURCE_RESULTS],
    #}
    return final_result
    

def lambda_handler(event, context):
    result = None
    action = int(event.get("action"))
    # meta_type = event.get("meta_type")
    # meta_id = int(event.get("meta_id"))

    if action == Action.TEST:
        result = test(meta_type, meta_id)
    elif action == Action.GENERATE:
        generate()

    return {"status": "OK", "result": result}
