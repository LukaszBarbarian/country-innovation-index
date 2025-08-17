import azure.functions as func
import logging

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)


@app.route(route="HttpTrigger1")
def HttpTrigger1(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            pass
        else:
            name = req_body.get('name')

    if name:
        return func.HttpResponse(
            f"Hello, {name}. This HTTP triggered function executed successfully.",
            mimetype="text/plain"
        )
    else:
        return func.HttpResponse(
            "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
            mimetype="text/plain"
        )