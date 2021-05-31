import logging
import json
import azure.functions as func
from services.apiDefinition import generate_kml


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        req_body = req.get_json()
    except ValueError:
        pass

    data = generate_kml(json.dumps(req_body))

    return func.HttpResponse(
        json.dumps(data), headers={"content-type": "application/json"},
        status_code=200
    )
