import os

from dotenv import load_dotenv


class EnvVarsHelper:

    is_env_loaded = False

    # Loads the environment variables in {project-root-folder}/tests/.env only the first time it is called
    @classmethod
    def load_test_environment_properties(cls):
        if not EnvVarsHelper.is_env_loaded:
            EnvVarsHelper.is_env_loaded = True
            env_vars_file_path = f'{os.path.dirname(os.path.abspath(__file__))}/../.env'
            load_dotenv(env_vars_file_path, override=True)
