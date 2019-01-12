import mongoengine as me
from utils import Config

cfg = Config()

class Db_connect():
    def __init__(self):
        me.connect(db=cfg.gphotos.database, alias=cfg.gphotos.collection, host=cfg.gphotos.host)
        me.connect(db=cfg.local.database, alias=cfg.local.database, host=None)


class Gphoto(me.Document):  # TODO: Remove strict: false from metadata once db is clean
    gid = me.StringField()
    imageMediaMetadata = me.DictField()
    md5Checksum = me.StringField()
    mimeType = me.StringField()
    name = me.StringField()
    originalFilename = me.StringField()
    ownedByMe = me.BooleanField()
    parents = me.ListField()
    gsize = me.IntField()
    trashed = me.BooleanField()
    meta = {
        'db_alias': cfg.gphotos.collection,
        'indexes': ['gid', 'md5Checksum'],
        'strict': False
    }


class Gphoto_state(me.Document):
    database_clean = me.BooleanField()
    start_token = me.StringField()
    meta = {'db_alias': cfg.gphotos.collection}


class Gphoto_parent(me.Document):
    gid = me.StringField()
    mimeType = me.StringField()
    name = me.StringField()
    ownedByMe = me.BooleanField()
    parents = me.ListField()
    trashed = me.BooleanField()
    path = me.ListField()
    meta = {'db_alias': cfg.gphotos.collection}


class Photo(me.Document):
    src_path = me.StringField(default=None)
    queue_path = me.StringField(default=None)
    size = me.IntField(default=None)
    md5sum = me.StringField(default=None)
    in_gphotos = me.BooleanField(default=False)
    queue_state = me.StringField(default=None, choices=['candidate', 'enqueued', 'done'])
    mirrored = me.BooleanField(default=False)
    gphotos_path = me.StringField(default=None)
    original_filename = me.StringField(default=None)
    gphoto_meta = me.DictField(default=None) #TODO:  Delete this?
    meta = {'allow_inheritance': True}

class Queue(Photo):
    meta = {'db_alias': cfg.local.database}

class Candidates(Photo):
    meta = {'db_alias': cfg.local.database}

class State(me.Document):
    target = me.StringField(default=None)
    old_target = me.StringField(default=None)
    dirlist = me.ListField(default=None)
    dirfilecount = me.IntField(default=0)
    dir_excluded_list = me.ListField(default=None)
    dirsize = me.IntField(default=0)
    dirtime = me.FloatField(default=0.0)
    mirror_ok = me.BooleanField(default=True)
    purge_ok = me.BooleanField(default=False)
    enqueue_ok = me.BooleanField(default=True)
    meta = {'db_alias': cfg.local.database}
