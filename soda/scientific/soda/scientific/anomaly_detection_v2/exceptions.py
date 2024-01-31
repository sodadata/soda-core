class PreprocessError(Exception):
    """Thrown in case of pre-processing.

    To be raised and passed as a result error message down the line.
    """


class AggregationValueError(Exception):
    """Thrown in case of wrong frequency aggregation.

    To be raised and passed as a result error message down the line.
    """


class FreqDetectionResultError(Exception):
    """Thrown in case of wrong frequency values.

    To be raised and passed as a result error message down the line.
    """


class NotSupportedHolidayCountryError(Exception):
    """Thrown in case of wrong holiday country.

    To be raised and passed as a result error message down the line.
    """


class WindowLengthError(Exception):
    """Thrown in case of wrong window length.

    To be raised and passed as a result error message down the line.
    """
