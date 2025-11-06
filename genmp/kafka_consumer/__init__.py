
import logging
import json
import azure.functions as func
from kafka_consumer.consumer import start, stop

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.function_name(name="ConsumerStart")
@app.route(route="start", auth_level=func.AuthLevel.ANONYMOUS)
async def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    state = req.params.get('state')
    if not state:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            state = req_body.get('state')

    if state:
        await start()
        return func.HttpResponse(f"Event sent to Event Hub, {state}. This event hub function executed successfully.")
    else:
        return func.HttpResponse(
             "This HTTP triggered function executed successfully. Pass a state in the query string or in the request body for a personalized response.",
             status_code=200
        )


@app.function_name(name="ConsumerStop")
@app.route(route="stop", auth_level=func.AuthLevel.ANONYMOUS)
async def stop_consumer(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Stop consumer request received.')
    try:
        await stop()
        return func.HttpResponse("Kafka consumer stopped successfully.", status_code=200)
    except Exception as e:
        logging.exception("Error stopping consumer")
        return func.HttpResponse(f"Error stopping consumer: {e}", status_code=500)
