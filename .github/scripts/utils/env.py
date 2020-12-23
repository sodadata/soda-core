import os


class ENV:

    @staticmethod
    def get_variable(name: str) -> str:
        value = os.environ.get(name, '')
        if value == '':
            raise Exception(f"No environment variable '{name}' has been defined!")
        return value

    @staticmethod
    def get_deployment_description():
        environment = os.environ.get('ENV', '')
        if environment == '':
            return ''
        else:
            return f"with deployment to *{environment}* environment"

    @classmethod
    def get_branch(cls):
        return os.path.basename(cls.get_variable('GITHUB_REF'))

    @classmethod
    def get_project(cls):
        return os.path.basename(cls.get_variable('GITHUB_REPOSITORY'))


    @classmethod
    def get_reports_url(cls):
        return get_variable('REPORTS_URL')
