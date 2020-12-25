import warnings


class Boto3Helper:

    @classmethod
    def filter_false_positive_boto3_warning(cls):
        # see
        # https://github.com/boto/boto3/issues/454#issuecomment-380900404
        warnings.filterwarnings("ignore", category=ResourceWarning, message='unclosed <ssl.SSLSocket')
        warnings.filterwarnings("ignore", category=DeprecationWarning, message='the imp module is deprecated')
        warnings.filterwarnings("ignore", category=DeprecationWarning, message='Using or importing the ABCs')
