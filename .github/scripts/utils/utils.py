import os
import urllib.parse


def get_environment_variable(name: str) -> str:
    value = os.environ.get(name, '')
    if value == '':
        raise Exception(f"No environment variable '{name}' has been defined!")
    return value


def get_deployment_description():
    environment = os.environ.get('ENV', '')
    if environment == '':
        return ''
    else:
        return f"with deployment to *{environment}* environment "


def get_test_reports_url():
    test_reports_base_url = get_environment_variable('TEST_REPORTS_BASE_URL')
    branch = os.path.basename(get_environment_variable('GITHUB_REF'))
    return f'{test_reports_base_url}/{urllib.parse.quote_plus(branch)}/'
