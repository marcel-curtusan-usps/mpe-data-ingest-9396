import logging
import json
import azure.functions as func
import asyncio
from function_app import app
from eventhubs.producer import start, stop, get_status
import os
import threading

@app.function_name(name="ProducerStart")
@app.route(auth_level=func.AuthLevel.ANONYMOUS)
async def producer_start(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Producer start request received.')
    try:
        started = await start()
        if started:
            return func.HttpResponse("Producer started", status_code=200)
        else:
            return func.HttpResponse("Producer already running", status_code=200)
    except Exception as e:
        logging.exception("Error starting producer")
        return func.HttpResponse(f"Error starting producer: {e}", status_code=500)


@app.function_name(name="ProducerStop")
@app.route(auth_level=func.AuthLevel.ANONYMOUS)
async def producer_stop(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Producer stop request received.')
    try:
        stopped = await stop()
        if stopped:
            return func.HttpResponse("Producer stopped", status_code=200)
        else:
            return func.HttpResponse("Producer was not running", status_code=200)
    except Exception as e:
        logging.exception("Error stopping producer")
        return func.HttpResponse(f"Error stopping producer: {e}", status_code=500)


@app.function_name(name="ProducerHealth")
@app.route(auth_level=func.AuthLevel.ANONYMOUS)
async def producer_health(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Producer health check received.')
    try:
        status = await get_status()
        return func.HttpResponse(json.dumps(status), status_code=200, mimetype="application/json")
    except Exception as e:
        logging.exception("Error checking producer health")
        return func.HttpResponse(f"Error checking producer health: {e}", status_code=500)

