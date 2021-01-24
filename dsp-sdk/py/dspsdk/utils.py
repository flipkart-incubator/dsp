import socket
import logging
import os

from hostsresolver import hostsfile_source as resolver

logger = logging.getLogger("utils")
logging.basicConfig(level=logging.INFO)


class Utils:
    """
        util class with static functions
    """

    @staticmethod
    def is_host_resolved(hostname):
        try:
            socket.gethostbyname(hostname)
            return True
        except socket.error:
            return False

    @staticmethod
    def write_to_file(file_path, data):
        with open(file_path,'w') as writer:
            writer.write(data)

    @staticmethod
    def delete_file(file_path):
        try:
            os.remove(file_path)
        except OSError:
            pass

    @staticmethod
    def resolve_host(file_path):
        resolver.install(file_path)

    @staticmethod
    def resolve_env_variable(variable_name):
        return os.environ.get(variable_name)
