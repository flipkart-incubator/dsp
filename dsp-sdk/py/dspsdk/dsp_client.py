import json
import requests
import logging

logger = logging.getLogger("dsp_client")
logging.basicConfig(level=logging.INFO)


class DspClient:
    """
        http client class for dsp
    """
    __config = {
        "base_uri": "http://0.0.0.0:9090"
    }
    headers = {"Content-Type": "application/json"}

    def __init__(self, base_url):
        if base_url is None:
            logger.info("dsp url set to default {}".format(self.__config["base_uri"]))
            pass
        else:
            self.__config["base_uri"] = base_url
            logger.info("dsp url set to {}".format(self.__config["base_uri"]))

    def get_blob_variables(self, request_id, blob_type):
        if request_id is None or blob_type is None:
            raise Exception("parameters request_id and blob_type are mandatory!")
        uri = self.__config["base_uri"] + "/v1/blob/all?request_id=" + request_id + "&type=" + blob_type
        req = requests.get(uri)
        response = req.text
        blob_variable = json.loads(response)
        if req.status_code != 200:
            return None
        return blob_variable["blobs"]

    def get_blob_info(self, request_id, blob_type):
        if request_id is None or blob_type is None:
            raise Exception("parameters request_id and blob_type are mandatory!")
        uri = self.__config["base_uri"] + "/v1/blob?request_id=" + request_id + "&type=" + blob_type
        req = requests.get(uri)
        response = req.text
        blob_info = json.loads(response)
        if req.status_code != 200:
            logger.info("dsp returned with {} for fetching blob".format(req.status_code))
            return None
        return blob_info

    def get_active_nn(self, cluster):
        if cluster is None:
            raise Exception("parameters cluster is mandatory!")
        uri = self.__config["base_uri"] + "/v1/active-nn?cluster=" + cluster
        req = requests.get(uri)
        response = req.text
        active_nn_response = json.loads(response)
        if req.status_code != 200:
            return None
        return active_nn_response["active_nn"]

    def register_blob(self, blob_payload):
        if blob_payload is None:
            raise Exception("blob_payload is mandatory!")
        uri = self.__config["base_uri"] + "/v1/blob"
        req = requests.post(uri, data=json.dumps(blob_payload), headers=self.headers)
        response = req.text
        blob_info = json.loads(response)
        if req.status_code != 200:
            logger.info("dsp returned with {} for registering blob".format(req.status_code))
            return None
        return blob_info

    def update_status_blob(self, blob_status_payload):
        if blob_status_payload is None:
            raise Exception("blob_status_payload is mandatory!")
        uri = self.__config["base_uri"] + "/v1/blob/status"
        req = requests.post(uri, data=json.dumps(blob_status_payload), headers=self.headers)
        response = req.text
        blob_info = json.loads(response)
        if req.status_code != 200:
            logger.info("dsp returned with {} for update status".format(req.status_code))
            return None
        return blob_info

    def get_cluster_hosts(self, cluster):
        if cluster is None:
            raise Exception("parameters cluster is mandatory!")
        uri = self.__config["base_uri"] + "/v1/cluster-hosts?cluster=" + cluster
        req = requests.get(uri)
        response = req.text
        active_nn_response = json.loads(response)
        if req.status_code != 200:
            return None
        return active_nn_response["nodes"]
