# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

# Patch all supported libraries including boto3
patch_all()

import boto3
import os
import json
import logging
import uuid
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_structured(request_id, message, **kwargs):
    """Helper function for structured logging"""
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "request_id": request_id,
        "message": message,
        **kwargs
    }
    logger.info(json.dumps(log_entry))


def handler(event, context):
    request_id = context.request_id
    table = os.environ.get("TABLE_NAME")
    
    log_structured(
        request_id,
        "Processing request",
        table_name=table,
        event_type="dynamodb_write"
    )
    
    try:
        if event.get("body"):
            item = json.loads(event["body"])
            log_structured(
                request_id,
                "Received payload",
                has_year="year" in item,
                has_title="title" in item,
                has_id="id" in item,
            )
            year = str(item["year"])
            title = str(item["title"])
            id = str(item["id"])
            dynamodb_client.put_item(
                TableName=table,
                Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
            )
            log_structured(
                request_id,
                "Successfully inserted data",
                operation="put_item"
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
        else:
            log_structured(
                request_id,
                "Received request without payload, using default data"
            )
            dynamodb_client.put_item(
                TableName=table,
                Item={
                    "year": {"N": "2012"},
                    "title": {"S": "The Amazing Spider-Man 2"},
                    "id": {"S": str(uuid.uuid4())},
                },
            )
            log_structured(
                request_id,
                "Successfully inserted default data",
                operation="put_item"
            )
            message = "Successfully inserted data!"
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": message}),
            }
    except Exception as e:
        log_structured(
            request_id,
            "Error processing request",
            error=str(e),
            error_type=type(e).__name__
        )
        raise
