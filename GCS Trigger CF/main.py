import os
import logging
from datetime import datetime
import functions_framework
from flask import jsonify,make_response
import pandas as pd


#-----------------Logger Setup-------------------------
logger=logging.getLogger('GCS Trigger')
logger.setLevel(logging.INFO)
handler=logging.StreamHandler()
handler.setFormatter(logging.Formatter(
    '%(asctime)s %(levelname)s %(name)s %(message)s'
))


if not logger.handlers:
    logger.addHandler(handler)

#----------------------------------------------------------

@functions_framework.cloud_event
def gcs_trigger_process(cloud_event):
    data=cloud_event.data
    bucket=data.get("bucket")
    name=data.get("name")


    if not name.startswith("raw-data-mnv/"):
        logger.info("skipping object not in raw data: %s",name)
        return make_response(jsonify({"skipped":"ignored non raw data path"}),204)

    if not name.endswith(".csv"):
        logger.info("skipping object not in raw data: %s",name)
        return make_response(jsonify({"skipped":"ignored non csv file"}),204)

    uri=f"gs://{bucket}/{name}"

    try:
        logger.info("reading from csv file")
        df=pd.read_csv(uri)

    except Exception as e:
        logger.error("failed to read csv")
        return make_response(jsonify({"error": str(e)}), 500)
    

    logger.info("Data sample:\n%s", df.head().to_string())
    record_count = len(df)
    logger.info("Total records read from %s: %d", name, record_count)

    return make_response(jsonify({"record count":record_count}),200)
