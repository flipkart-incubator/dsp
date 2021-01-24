import logging
import pickle
import hdfs
import pandas
import yaml

logger = logging.getLogger("hdfs_client")
logging.basicConfig(level=logging.INFO)


class HdfsClient:
    """
        hdfs client class using web-hdfs
    """
    __active_nn = "prod-hadoop-hadoopcluster2-nn-0002"
    __port = "50070"
    __hdfs_user = "fk-ip-data-service"
    __hdfs_client = None

    def __init__(self, hdfs_user, active_nn, port):
        if hdfs_user is not None:
            self.__hdfs_user = hdfs_user
        if active_nn is not None:
            self.__active_nn = active_nn
        if port is not None:
            self.__port = port
        self.__get_connection()

    def __get_connection(self):
        try:
            self.__hdfs_client = hdfs.InsecureClient("http://"+self.__active_nn+":"+self.__port, user=self.__hdfs_user)
            logger.debug("established connection with hdfs!")
        except Exception as error:
            logger.error("cannot establish connection with hdfs!")
            raise error

    def list_files(self, file_path):
        return self.__hdfs_client.list(file_path)

    def delete_dir(self, dir_path):
        return self.__hdfs_client.delete(dir_path, recursive=True)

    def read_pickle_object(self, file_path):
        with self.__hdfs_client.read(file_path) as reader:
            pkl_object = pickle.load(reader)
            return pkl_object

    def write_pickle_object(self, file_path, data):
        self.__hdfs_client.delete(file_path)
        with self.__hdfs_client.write(file_path) as writer:
            pickle.dump(data, writer)

    def read_csv_object(self, file_path):
        with self.__hdfs_client.read(file_path) as reader:
            df_object = pandas.read_csv(reader, encoding='utf-8')
            return df_object

    def write_csv_object(self, file_path, data):
        if not isinstance(data, pandas.DataFrame):
            raise Exception("CSV only support pandas dataframe!")
        self.__hdfs_client.delete(file_path)
        with self.__hdfs_client.write(file_path, encoding='utf-8') as writer:
            data.to_csv(writer, header=True, index=False)

    def read_yml_object(self, file_path):
        with self.__hdfs_client.read(file_path, encoding='utf-8') as reader:
            yml_object = yaml.load(reader, Loader=yaml.FullLoader)
            return yml_object

    def write_yml_object(self, file_path, data):
        self.__hdfs_client.delete(file_path)
        with self.__hdfs_client.write(file_path, encoding='utf-8') as writer:
            yaml.dump(data, writer)
