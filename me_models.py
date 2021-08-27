import mongoengine as me
from utils import config

cfg = config()


class DbConnect:
    def __init__(self):
        me.connect(
            db=cfg.gphotos.database, alias=cfg.gphotos.collection, host=cfg.gphotos.host
        )
        me.connect(
            db=cfg.local.database,
            alias=cfg.local.database,
            host='labroidgce.mooo.com',
            username='scott',
            password='mongo14',
            authentication_source='admin')
        me.connect(
            db=cfg.source_archive.database,
            alias=cfg.source_archive.database,
            host=cfg.source_archive.host,
        )


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
    path = me.ListField()
    meta = {
        "db_alias": cfg.gphotos.collection,
        "indexes": ["gid", "md5Checksum"],
        "strict": False,
    }


class GphotoState(me.Document):
    database_clean = me.BooleanField()
    start_token = me.StringField()
    meta = {"db_alias": cfg.gphotos.collection}


class Src_metadata(me.EmbeddedDocument):
    cameraMake = me.StringField(default=None)
    cameraModel = me.StringField(default=None)
    creationTime = me.DateTimeField(default=None)
    dateTimeOriginal = me.DateTimeField(default=None)
    width = me.IntField(default=0)
    height = me.IntField(default=0)


class Photo(me.Document):
    src_path = me.StringField(default=None)
    size = me.IntField(default=None)
    modifiedTime = me.DateTimeField(default=None)
    md5sum = me.StringField(default=None)
    image_md5 = me.StringField(default=None)
    src_metadata = me.EmbeddedDocumentField(Src_metadata)
    gid = me.StringField(default=None)
    in_gphotos = me.BooleanField(default=False)
    in_process = me.BooleanField(default=False)
    mirrored = me.BooleanField(default=False)
    purged = me.BooleanField(default=False)
    # uploaded = me.BooleanField(default=False)
    # uploading = me.BooleanField(default=False)
    # upload_tries = me.IntField(default=0)
    # upload_elapsed = me.FloatField(default=0)
    # gphotos_path = me.ListField(default=None)
    src_filename = me.StringField(default=None)
    takeout_path = me.StringField(default=None)
    in_takeout = me.BooleanField(default=False)
    original_filename = me.StringField(default=None)
    gphoto_meta = me.DictField(default=None)  # TODO:  Delete this?
    meta = {"allow_inheritance": True}


class Queue(Photo):
    meta = {"db_alias": cfg.local.database}


class Candidates(me.Document):
    src_path = me.StringField(default=None)
    md5sum = me.StringField(default=None)
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
    upload_ok = me.BooleanField(default=False)
    status = me.ListField(default=[])
    meta = {"db_alias": cfg.local.database}


class SourceArchive(me.Document):
    md5sum = me.StringField(default=None, unique=True)
    paths = me.ListField(default={})
    meta = {"db_alias": cfg.source_archive.database, "indexes": ["md5sum"]}
