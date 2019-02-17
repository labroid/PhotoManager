import mongoengine as me
from utils import config

cfg = config()


class Db_connect:
    def __init__(self):
        me.connect(
            db=cfg.gphotos.database, alias=cfg.gphotos.collection, host=cfg.gphotos.host
        )
        me.connect(db=cfg.local.database, alias=cfg.local.database, host=None)
        me.connect(
            db=cfg.path_history.database,
            alias=cfg.path_history.database,
            host=cfg.path_history.host,
        )


class Gphoto(me.Document):  # TODO: Remove strict: false from metadata once db is clean
    gid = me.StringField()
    imageMediaMetadata = me.DictField()
    md5Checksum = me.StringField(unique=True)
    mimeType = me.StringField()
    name = me.StringField()
    originalFilename = me.StringField()
    ownedByMe = me.BooleanField()
    parents = me.ListField()
    gsize = me.IntField()
    trashed = me.BooleanField()
    path = me.ListField()
    meta = {
        "db_alias": cfg.gphotos.collection,
        "indexes": ["gid", "md5Checksum"],
        "strict": False,
    }


class Gphoto_state(me.Document):
    database_clean = me.BooleanField()
    start_token = me.StringField()
    meta = {"db_alias": cfg.gphotos.collection}


# class Gphoto_parent(me.Document):  # Depricated?
#     gid = me.StringField()
#     mimeType = me.StringField()
#     name = me.StringField()
#     ownedByMe = me.BooleanField()
#     parents = me.ListField()
#     trashed = me.BooleanField()
#     path = me.ListField()
#     meta = {"db_alias": cfg.gphotos.collection}


class Photo(me.Document):
    src_path = me.StringField(default=None)
    size = me.IntField(default=None)
    md5sum = me.StringField(default=None)
    gid = me.StringField(default=None)
    in_gphotos = me.BooleanField(default=False)
    mirrored = me.BooleanField(default=False)
    purged = me.BooleanField(default=False)
    gphotos_path = me.ListField(default=None)
    original_filename = me.StringField(default=None)
    gphoto_meta = me.DictField(default=None)  # TODO:  Delete this?
    meta = {"allow_inheritance": True}


class Queue(Photo):
    meta = {"db_alias": cfg.local.database}


class Candidates(Photo):
    meta = {"db_alias": cfg.local.database}


class State(me.Document):
    target = me.StringField(default=None)
    old_target = me.StringField(default=None)
    dirlist = me.ListField(default=[])
    dirfilecount = me.IntField(default=0)
    excluded_ext_dict = me.DictField(default={})
    dirsize = me.IntField(default=0)
    dirtime = me.FloatField(default=0.0)
    mirror_ok = me.BooleanField(default=True)
    mirror_root = me.StringField(default="")
    purge_ok = me.BooleanField(default=False)
    enqueue_ok = me.BooleanField(default=True)
    meta = {"db_alias": cfg.local.database}


class SourceList(me.Document):
    md5sum = me.StringField(default=None, unique=True)
    paths = me.ListField(default={})
    meta = {"db_alias": cfg.path_history.database, "indexes": ["md5sum"]}
