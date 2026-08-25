class HTTPError(Exception):
    status_code: int = 500
    description: str = None

    def __init__(self, status_code: int, description: str = None):
        self.status_code = status_code
        self.description = description
        super().__init__(self.status_code, self.description)

error_messages = {
    400: 'Invalid request.',
    401: 'Unauthorized.',
    403: 'Forbidden.',
    404: 'File not found.',
    405: 'Method not allowed.',
    416: 'Requested range not satisfiable.',
    500: 'Internal server error.'
}

async def invalid_request(_):
    return 'Invalid request.', 400

async def not_found(_):
    return 'Resource not found.', 404

async def invalid_method(_):
    return 'Invalid request method.', 405

async def http_error(error: HTTPError):
    error_message = error_messages.get(error.status_code, 'Error')
    return error.description or error_message, error.status_code

def abort(status_code: int = 500, description: str = None):
    raise HTTPError(status_code, description)
