from abc import ABC, abstractmethod

from soda.contract.parser.legacy.parser_file import ParserFile


class ParserPlugin(ABC):

    """
    There is one plugin per parser.  The parser plugin will have to store all the parsed information
    internally for all files.

    The plugin can use
     * file.... the already parsed fields in the given file
     * file.root_yaml_object to parse all the plugin specific yaml
     * file.logs for logging
    """

    @abstractmethod
    def parse(self, file: ParserFile) -> None:
        pass
