#  Copyright 2020 Soda
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#   http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import logging
import os
from pathlib import Path


class FileSystem:

    def join(self, a: str, *paths: str):
        return os.path.join(a, *paths)

    def dirname(self, path: str):
        return os.path.dirname(path)

    def normpath(self, path):
        return os.path.normpath(path)

    def split(self, path: str):
        return os.path.split(path)

    def file_exists(self, path: str):
        expanded_path = os.path.expanduser(path)
        return Path(expanded_path).exists()

    def is_dir(self, path: str):
        expanded_path = os.path.expanduser(path)
        return Path(expanded_path).is_dir()

    def is_file(self, path):
        expanded_path = os.path.expanduser(path)
        return Path(expanded_path).is_file()

    def is_readable(self, path: str):
        return os.access(path, os.R_OK)

    def list_dir(self, dir_path):
        return os.listdir(dir_path)

    def mkdirs(self, path: str):
        expanded_path = os.path.expanduser(path)
        Path(expanded_path).mkdir(parents=True, exist_ok=True)

    def user_home_dir(self):
        return str(Path.home())

    def file_read_as_str(self, path: str) -> str:
        expanded_path = os.path.expanduser(path)
        try:
            with open(expanded_path) as f:
                return f.read()
        except Exception as e:
            logging.debug(f"Couldn't read {str(path)}: {str(e)}")

    def file_write_from_str(self, path: str, file_content_str):
        expanded_path = os.path.expanduser(path)
        path_path: Path = Path(expanded_path)
        is_new = not path_path.exists()
        try:
            with open(path_path, 'w+') as f:
                f.write(file_content_str)
            if is_new:
                os.chmod(path, 0o666)
        except Exception as e:
            logging.debug(f"Couldn't write {str(path)}: {str(e)}")

    def is_readable_file(self, file_path: str):
        if not self.file_exists(file_path):
            logging.error(f'{file_path} does not exist')
            return False
        elif not self.is_file(file_path):
            logging.error(f'{file_path} is not a file')
            return False
        elif not self.is_readable(file_path):
            logging.error(f'{file_path} is not readable')
            return False
        return True


class FileSystemSingleton:
    INSTANCE: FileSystem = FileSystem()


def file_system():
    return FileSystemSingleton.INSTANCE
