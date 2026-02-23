class ControllerError(Exception):
    """Raised by controllers to signal an error message to the view."""

    def __init__(self, message):
        self.message = message
        super().__init__(message)
