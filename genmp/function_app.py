import azure.functions as func
app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Import package submodules explicitly to ensure imports resolve when loaded by Azure Functions
import eventhubs  # noqa: F401
import kafka_consumer  # noqa: F401