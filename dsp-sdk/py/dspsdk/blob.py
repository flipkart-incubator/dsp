import logging
import pandas

from .hdfs_client import HdfsClient
from .dsp_client import DspClient
from .utils import Utils
from .blob_exceptions import DSPClientError, BlobError

logger = logging.getLogger("blob")
logging.basicConfig(level=logging.INFO)


class Blob:
    """
        blob util class for dsp
    """
    __request_id = None
    __type = "PY3"
    __NAME_NODE = "prod-hadoop-hadoopcluster2-nn-0002"

    def __init__(self, request_id):
        if request_id is None:
            raise Exception("parameters request_id is mandatory!")
        self.__request_id = request_id
        self.__get_dsp_client()
        self.__get_active_nn()
        self.__populate_hostnames()
        self.__get_hdfs_client()
        self.__set_hdfs_path_safely()

    def __set_hdfs_path_safely(self):
        retry_count = 3
        while retry_count > 0:
            try:
                self.__set_hdfs_path()
                break
            except DSPClientError:
                retry_count = retry_count - 1
                if retry_count == 0:
                    raise BlobError("failed to initialise blob!")

    def __set_hdfs_path(self):
        # fetch existing blob
        blob_meta = self.__dsp_client.get_blob_info(self.__request_id, self.__type)
        if blob_meta is not None:
            self.__hdfs_path = blob_meta['location'].replace("hdfs://hadoopcluster2", "")
            return
        # register new blob on dsp
        else:
            blob_meta_payload = dict()
            blob_meta_payload['request_id'] = self.__request_id
            blob_meta_payload['type'] = self.__type
            blob_meta_payload['status'] = "started"
            blob_meta = self.__dsp_client.register_blob(blob_meta_payload)
            if blob_meta is not None:
                self.__hdfs_path = blob_meta['location'].replace("hdfs://hadoopcluster2", "")
                return
        raise DSPClientError("Blob initiation failed!")

    def __get_hdfs_client(self):
        hadoop_user = Utils.resolve_env_variable("DSP_SDK_HADOOP_USER")
        hadoop_port = Utils.resolve_env_variable("DSP_SDK_HADOOP_PORT")
        self.__hdfs_client = HdfsClient(hadoop_user, self.__active_nn, hadoop_port)

    def __get_dsp_client(self):
        dsp_url = Utils.resolve_env_variable("DSP_SDK_BASE_URL")
        self.__dsp_client = DspClient(dsp_url)

    def __get_active_nn(self):
        # fetch active_nn from dsp ws
        self.__active_nn = self.__dsp_client.get_active_nn("hadoopcluster2")

    def __populate_hostnames(self):
        if Utils.is_host_resolved(self.__NAME_NODE):
            logger.info("using system hosts!")
            pass
        else:
            logger.info("resolving cluster hosts!")
            file_path = "hdfs-hosts"
            hosts = self.__dsp_client.get_cluster_hosts("hadoopcluster2")
            Utils.write_to_file(file_path, hosts)
            Utils.resolve_host(file_path)
            Utils.delete_file(file_path)

    def __get_hdfs_file_path(self):
        return self.__hdfs_path.replace(self.__type, "ALL")

    def load(self, variable_name):
        retry_count = 2
        while retry_count > 0:
            try:
                variable_path = self.__hdfs_path + variable_name
                return self.__hdfs_client.read_pickle_object(variable_path)
            except Exception:
                retry_count = retry_count - 1
                if retry_count == 0:
                    raise BlobError("failed to load blob!")

    def dump(self, variable_name, object_data):
        retry_count = 2
        while retry_count > 0:
            try:
                variable_path = self.__hdfs_path + variable_name
                self.__hdfs_client.write_pickle_object(variable_path, object_data)
                break
            except Exception:
                retry_count = retry_count - 1
                if retry_count == 0:
                    raise BlobError("failed to dump blob!")

    def list_all(self):
        var_list = self.__dsp_client.get_blob_variables(self.__request_id, self.__type)
        if var_list is None:
            return []
        return var_list

    def load_file(self, variable_name, file_type):
        retry_count = 3
        while retry_count > 0:
            try:
                variable_path = self.__get_hdfs_file_path() + variable_name
                if file_type == "CSV":
                    return self.__chunk_reads(variable_path)
                elif file_type == "YAML":
                    return self.__hdfs_client.read_yml_object(variable_path)
                else:
                    raise BlobError("valid type allowed: CSV, YAML")
            except Exception:
                retry_count = retry_count - 1
                if retry_count == 0:
                    raise BlobError("failed to load file blob!")

    def dump_file(self, variable_name, object_data, file_type):
        retry_count = 3
        while retry_count > 0:
            try:
                variable_path = self.__get_hdfs_file_path() + variable_name
                if file_type == "CSV":
                    self.__hdfs_client.delete_dir(variable_path)
                    self.__chunk_writes(variable_path, object_data, 200000)
                elif file_type == "YAML":
                    self.__hdfs_client.write_yml_object(variable_path, object_data)
                else:
                    raise BlobError("valid file_type allowed: CSV, YAML")
                break
            except Exception:
                retry_count = retry_count - 1
                if retry_count == 0:
                    raise BlobError("failed to dump file blob!")

    def __chunk_writes(self, variable_path, object_data , chunk_size):
        chunk_size = len(object_data)
        num_chunks = 1
        for i in range(num_chunks):
            chunk_file_path = variable_path + "/chunk_" + str(i)
            chunk = object_data[i * chunk_size:(i + 1) * chunk_size]
            retry_count = 3
            while retry_count > 0:
                try:
                    self.__hdfs_client.write_csv_object(chunk_file_path, chunk)
                    break
                except Exception:
                    retry_count = retry_count - 1
                    if retry_count == 0:
                        raise BlobError("failed to dump file chunk!")

    def __chunk_reads(self, variable_path):
        files = self.__hdfs_client.list_files(variable_path)
        dfList = []
        for file in files:
            chunk_file = variable_path + "/" + file
            retry_count = 3
            while retry_count > 0:
                try:
                    dfList.append(self.__hdfs_client.read_csv_object(chunk_file))
                    break
                except Exception:
                    retry_count = retry_count - 1
                    if retry_count == 0:
                        raise BlobError("failed to dump file chunk!")
        return pandas.concat(dfList, ignore_index=True)

    def list_all_files(self):
        var_list = self.__dsp_client.get_blob_variables(self.__request_id, "ALL")
        if var_list is None:
            return []
        return var_list
