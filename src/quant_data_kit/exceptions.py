"""quant_data_kit exceptions."""


class QuantDataKitError(Exception):
    """Base error for quant-data-kit."""


class ValidationError(QuantDataKitError):
    """Raised when a dataframe fails schema or quality checks."""


class ProviderError(QuantDataKitError):
    """Raised when a data provider fails to fetch data."""
