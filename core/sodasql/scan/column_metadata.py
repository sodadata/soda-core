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

class ColumnMetadata:

    def __init__(self, name: str, data_type: str = None, semantic_type: str = None, nullable: bool = None):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.semantic_type = semantic_type

    def __str__(self):
        return self.name + (' ' + self.type if self.type else '')

    def to_json(self):
        return {
            'name': self.name,
            # TODO kept backward compatibility, remove after https://github.com/sodadata/soda/issues/2385 is fixed
            'type': self.data_type,
            'dataType': self.data_type,
            'nullable': self.nullable,
            'semanticType': self.semantic_type,
        }
