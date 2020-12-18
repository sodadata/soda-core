import os


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
