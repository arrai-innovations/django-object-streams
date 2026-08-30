"""Domain exceptions for object stream registration and evaluation."""


class ObjectStreamsError(Exception):
    """Base exception for package-specific errors."""


class RegistrationError(ObjectStreamsError):
    """Base exception for registry errors."""


class AlreadyRegistered(RegistrationError):
    """Raised when a model is registered more than once."""


class NotRegistered(RegistrationError):
    """Raised when a requested model has no object stream registration."""


class FilterValidationError(ObjectStreamsError):
    """Raised when subscription filter parameters fail FilterSet validation."""

    def __init__(self, errors):
        self.errors = errors
        super().__init__(str(errors))
