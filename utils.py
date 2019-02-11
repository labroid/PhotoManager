import hashlib
import logging
import yaml
import os
from dataclasses import dataclass


def file_md5sum(path):
    BUF_SIZE = 65536

    md5 = hashlib.md5()
    # try:
    with open(path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            md5.update(data)
    return md5.hexdigest()

    # except IOError:


    # try:
    #     f = open(path, "rb")
    # except IOError:
    #     logging.error("Can't open path {}".format(path))
    #     return ''
    # else:
    #     with f:
    #         while True:
    #             data = f.read(BUF_SIZE)
    #             if not data:
    #                 break
    #             md5.update(data)
    # return md5.hexdigest()


def stat_node(nodepath):
    """stat node and return file stats as os.stat object"""
    try:
        file_stat = os.stat(nodepath)
    except:
        error_message = "Can't stat file at {0}".format(repr(nodepath))
        logging.error(error_message)
        raise ValueError(error_message)
    return file_stat


# def cfg_obj(config, key):
#     Obj_cls = collections.namedtuple('Obj_cls', config[key].keys())
#     return Obj_cls(**config[key])


def get_yaml():
    with open("config.yaml") as f:
        config_dict = yaml.safe_load(f.read())
    return config_dict


# class Config():
#     def __init__(self):
#         self.__cfg = get_cfg()
#         self.local = cfg_obj(self.__cfg, 'local')
#         self.gphotos = cfg_obj(self.__cfg, 'gphotos')
#         self.logging = self.__cfg['logging']


@dataclass
class Settings:
    purge_ok: bool = False
    mirror_ok: bool = False


@dataclass
class Local:
    gphoto_upload_queue: str
    mirror_root: str
    image_filetypes: list
    log_file_base: str
    mongod_path: str
    database: str


@dataclass
class Gphotos:
    host: str
    database: str
    collection: str
    gphoto_db_alias: str


@dataclass
class PathHistory:
    host: str
    database: str
    collection: str


@dataclass
class Cfg:
    settings: Settings
    local: Local
    gphotos: Gphotos
    path_history: PathHistory


def config():
    with open("config.yaml") as f:
        config_dict = yaml.safe_load(f.read())
    cfg = Cfg(
        settings=Settings(**config_dict["settings"]),
        local=Local(**config_dict["local"]),
        gphotos=Gphotos(**config_dict["gphotos"]),
        path_history=PathHistory(**config_dict["path_history"]),
    )
    return cfg
