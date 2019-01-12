import hashlib
import collections
import logging
import yaml
import os
import mongoengine


def file_md5sum(path):
    BUF_SIZE = 65536

    md5 = hashlib.md5()
    try:
        f = open(path, 'rb')
    except IOError:
        logging.error("Can't open path {}".format(path))
    else:
        with f:
            while True:
                data = f.read(BUF_SIZE)
                if not data:
                    break
                md5.update(data)
    return md5.hexdigest()


def stat_node(nodepath):
    """stat node and return file stats as os.stat object"""
    try:
        file_stat = os.stat(nodepath)
    except:
        error_message = "Can't stat file at {0}".format(repr(nodepath))
        logging.error(error_message)
        raise (ValueError, error_message)
    return file_stat


def cfg_obj(config, key):
    Obj_cls = collections.namedtuple('Obj_cls', config[key].keys())
    return Obj_cls(**config[key])


def get_cfg():
    with open("config.yaml") as f:
        config = yaml.safe_load(f.read())
    return config


class Config():
    def __init__(self):
        self.__cfg = get_cfg()
        self.local = cfg_obj(self.__cfg, 'local')
        self.gphotos = cfg_obj(self.__cfg, 'gphotos')
        self.logging = self.__cfg['logging']
