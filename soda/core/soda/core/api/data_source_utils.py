from soda.scan import Scan


class DataSourceUtils:
    """
    EXPERIMENTAL: Utility API for DataSource and connections.
    """

    def __init__(self, config_path: str):
        """
        All the features for config files (e.g. env vars) are supported.
        :param config_path: The director path containing the data source config yaml files.
        """
        self.config_path = config_path
        self._scan = Scan()
        self._scan.add_configuration_yaml_files(self.config_path)

    def get_connection(self, data_source_name: str):
        """
        Returns a DB API connection object configured for the given data source name
        :param data_source_name: Name of a data source configured in one of the self.config_path files.
        :return: connection object
        """
        ds = self._scan._data_source_manager.get_data_source(data_source_name)
        if ds is None:
            raise Exception(self._scan.get_logs_text())
        else:
            ds.connect()
            return ds.connection
