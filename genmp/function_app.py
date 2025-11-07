import azure.functions as func
import json
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Import package submodules explicitly to ensure imports resolve when loaded by Azure Functions
import eventhubs  # noqa: F401
import kafka_consumer  # noqa: F401


@app.function_name(name="Status")
@app.route(route="status", auth_level=func.AuthLevel.ANONYMOUS)
async def status(req: func.HttpRequest) -> func.HttpResponse:
    """Return a simple HTML page or JSON status for humans and machines.

    - If the Accept header includes application/json, return JSON.
    - Otherwise return a small HTML page with links to function endpoints.
    """
    # basic static links — updated to reflect the cleaner proxy routes
    functions = {
        "ProducerHealth": "/producer/health",
        "ProducerStart": "/producer/start",
        "ProducerStop": "/producer/stop",
        "ConsumerStart": "/consumer/start",
        "ConsumerStop": "/consumer/stop",
    }

    # Gather minimal runtime info
    info = {
        "app": "genmp",
        "env": req.headers.get("x-functions-environment") or (req.headers.get("Host") or "localhost"),
        "functions": functions,
    }

    accept = req.headers.get("Accept", "")
    if "application/json" in accept:
        return func.HttpResponse(body=json.dumps(info), status_code=200, mimetype="application/json")

    # Build a simple HTML page
    links_html = "\n".join([f'<li><a href="{v}">{k}</a></li>' for k, v in functions.items()])
    html = f"""
    <html>
        <head><title>genmp status</title></head>
        <body>
            <h1>genmp status</h1>
            <p>Basic status and shortcuts:</p>
            <ul>
                {links_html}
            </ul>
            <pre>{json.dumps(info, indent=2)}</pre>
        </body>
    </html>
    """
    return func.HttpResponse(html, status_code=200, mimetype="text/html")


@app.function_name(name="ProducerHealthProxy")
@app.route(route="producer/health", auth_level=func.AuthLevel.ANONYMOUS)
async def producer_health_proxy(req: func.HttpRequest) -> func.HttpResponse:
    """Proxy endpoint that returns the same JSON as the producer health function at /producer/health."""
    try:
        # Import here to avoid circular import issues during module load
        from eventhubs.producer import get_status as _get_status  # type: ignore
        status = await _get_status()
        return func.HttpResponse(body=json.dumps(status), status_code=200, mimetype="application/json")
    except Exception as e:
        logging.exception("Error in producer_health_proxy")
        return func.HttpResponse(f"Error: {e}", status_code=500)
