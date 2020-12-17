import os


def get_env(env_name: str) -> str:
    value = os.environ.get(env_name, '')
    if value == '':
        raise Exception(f"no environment variable {env_name} defined!")
    return value


def deployment_description():
    environment = os.environ.get('ENV', '')
    if environment == '':
        return ''
    else:
        return f"with deployment to *{environment}* environment "
