import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Import package submodules explicitly to ensure imports resolve when loaded by Azure Functions
from genmp import eventhubs  # noqa: F401
from genmp import kafka_consumer  # noqa: F401