import warnings
import os.path
import mongoengine as me
import datetime
import functools
from dataclasses import dataclass
import oauth2creds
from googleapiclient.discovery import build
import logging
from logging.config import dictConfig

from me_models import Gphoto, Gphoto_state
from utils import Config

FOLDER = "application/vnd.google-apps.folder"
FILE_FIELDS = "id,imageMediaMetadata/time,md5Checksum,mimeType,name,originalFilename,ownedByMe,parents,size,trashed"
INIT_FIELDS = f"files({FILE_FIELDS}), nextPageToken"
UPDATE_FIELDS = f"changes(file({FILE_FIELDS}),fileId,removed),nextPageToken"
MIME_FILTER = ["image", "video", "application/vnd.google-apps.folder"]

cfg = Config()
service = build("drive", "v3", credentials=oauth2creds.get_credentials())
me.connect(
    db=cfg.gphotos.database, host=cfg.gphotos.host, alias=cfg.gphotos.gphoto_db_alias
)


def main():
    gsync = GphotoSync()
    gsync.sync()


class GphotoSync:
    def __init__(self):
        dictConfig(cfg.logging)
        self.log = logging.getLogger(__name__)
        self.root = None

    def sync(self):
        if self.database_clean() and self.start_token() is not None:
            self.update_db()
        else:
            self.log.info("Database dirty: Rebulding")
            self.rebuild_db()
        self.start_token(update=True)

    def rebuild_db(self):
        start_time = datetime.datetime.now()
        self.database_clean(set_state=False)
        Gphoto.drop_collection()
        self.get_root()
        self.walk(folder=self.root)
        self.database_clean(set_state=True)
        self.log.info(
            f"Full resync elapsed time: {datetime.datetime.now() - start_time}"
        )

    def get_root(self):
        root_id = service.files().get(fileId="root").execute().get("id")
        assert root_id is not None, "No root ID found for Google Drive"
        node_dict = self.steralize(
            service.files()
            .list(
                q=f"parents in '{root_id}' and trashed = false and name = 'google photos'",
                fields=INIT_FIELDS,
            )
            .execute()
            .get("files")[0]
        )
        root = Gphoto(**node_dict)
        Gphoto.objects(gid=root.gid).update(upsert=True, **node_dict)
        self.root = root
        return

    def walk(self, folder, path=None):
        path = path or []
        folders = []
        db_nodes = []
        path.append(folder.name)
        self.log.info(f"Path: {path}")
        for node in self.get_nodes(folder):
            node.path = path
            if node.mimeType == FOLDER:
                folders.append(node)
            db_nodes.append(node)
        if db_nodes:
            Gphoto.objects.insert(db_nodes)
        for folder in folders:
            self.walk(folder, path)
        path.pop()

    def get_nodes(self, parent):
        cumulative = 0
        nodes = []
        nextpagetoken = None
        query = f"'{parent.gid}' in parents and (mimeType contains 'image/' or mimeType contains 'video/' or mimeType = 'application/vnd.google-apps.folder') and trashed = false"
        while True:
            start_time = datetime.datetime.now()
            response = (
                service.files()
                .list(
                    q=query, pageSize=1000, pageToken=nextpagetoken, fields=INIT_FIELDS
                )
                .execute()
            )
            elapsed = datetime.datetime.now() - start_time
            count = len(response["files"])
            cumulative += count
            self.log.info(
                f"{elapsed} Drive delivered {count} files. Total: {cumulative}"
            )
            sterile_nodes = [self.steralize(x) for x in response["files"]]
            nodes += [Gphoto(**x) for x in sterile_nodes]
            nextpagetoken = response.get("nextPageToken")
            if nextpagetoken is None:
                return nodes

    def start_token(self, update=False):
        if update:
            start_token = service.changes().getStartPageToken().execute()
            Gphoto_state.objects().update_one(
                upsert=True, start_token=start_token["startPageToken"]
            )
            return start_token
        else:
            try:
                start_token = Gphoto_state.objects().get().start_token
            except me.MultipleObjectsReturned:
                raise me.MultipleObjectsReturned(
                    "More than one record in State. Should never happen"
                )
            except me.DoesNotExist:
                return None
            return start_token

    def get_changes(self):
        """
        Google API for changes().list() returns:
        {
            "kind": "drive#changeList",
            "nextPageToken": string,
            "newStartPageToken": string,
            "changes": [
                changes Resource
            ]
        }

        where a changes Resource is:

        {
            "kind": "drive#change",
            "type": string,
            "time": datetime,
            "removed": boolean,
            "fileId": string,
            "file": files Resource,
            "teamDriveId": string,
            "teamDrive": teamdrives Resource
        }

        """
        change_token = Gphoto_state.objects().get()["start_token"]
        changes = []
        while True:
            response = (
                service.changes()
                .list(
                    pageToken=change_token,
                    pageSize=1000,
                    includeRemoved=True,
                    fields=UPDATE_FIELDS,
                )
                .execute()
            )
            self.log.info(
                f"Google sent {len(response.get('changes', []))} change records"
            )
            changes += response["changes"]
            change_token = response.get("nextPageToken")
            if change_token is None:
                break
        return changes

    @dataclass
    class DriveChange:
        removed: bool
        fileId: str
        file: dict
        gphoto = None
        kind: str = None
        type: str = None
        time: datetime = None
        teamDriveId: str = None
        teamDrive: str = None
        #
        # def __post_init__(self):
        #     self.gphoto = Gphoto(**self.file)

    def validate_drive_changes(self, drive_changes):
        valid_changes = []
        for change in drive_changes:
            drive_change = self.DriveChange(**change)
            drive_change.gphoto = Gphoto(**self.steralize(drive_change.file))
            if not (
                "image/" in drive_change.gphoto.mimeType
                or "video/" in drive_change.gphoto.mimeType
                or drive_change.gphoto.mimeType == "application/vnd.google-apps.folder"
            ):
                continue
            if (
                not os.path.splitext(drive_change.gphoto.name)[1]
                in cfg.local.image_filetypes
            ):
                continue
            drive_change.gphoto.path = self.get_node_path(drive_change.gphoto)
            valid_changes.append(drive_change)
        return valid_changes

    def update_db(self):
        drive_changes = self.get_changes()
        photo_changes = self.validate_drive_changes(drive_changes)
        if not photo_changes:
            self.log.info("No changes to photos detected")
            return
        delete_count = new_count = 0
        self.database_clean(set_state=False)
        for change in photo_changes:
            if change["removed"] or change["file"]["trashed"]:
                try:
                    Gphoto.objects(gid=change["fileId"]).get()
                except me.errors.DoesNotExist:
                    self.log.info(
                        f"Record for removed file ID {change['fileId']} not in database. Moving on..."
                    )
                    continue
                except me.errors.MultipleObjectsReturned:
                    self.log.info(
                        f"Record for removed file ID {change['fileId']} returned multiple hits in database. Consider rebuilding database."
                    )
                    raise me.errors.MultipleObjectsReturned(
                        "Multiple records with ID {change['fileId']} in database. Consider rebuilding database."
                    )
                self.log.info(
                    f"Removing record for file ID {change['fileId']} from database."
                )
                Gphoto.objects(gid=change["fileId"]).delete()
                delete_count += 1
                continue
            if not any(
                mimeType in change["file"]["mimeType"] for mimeType in MIME_FILTER
            ):
                self.log.info(
                    f"Skipping {change['file']['name']} of mimeType {change['file']['mimeType']}'"
                )
                continue
            self.log.info(f"Updating record {change['file']['name']}")
            change["file"] = self.steralize(change["file"])
            if not change["file"].get("parents", None):
                warn_str = f"Parents list empty for ID {change['file']['gid']} - something is strange."
                self.log.info(warn_str)
                warnings.warn(warn_str)
            Gphoto.objects(gid=change["file"]["gid"]).update_one(
                upsert=True, **change["file"]
            )
            new_count += 1
        self.set_paths()
        # self.purge_nodes_outside_roots()
        self.database_clean(set_state=True)
        self.log.info(
            f"Sync update complete. New file count: {new_count} Deleted file count: {delete_count}"
        )

    def set_paths(self):
        orphans = Gphoto.objects(path=[])
        print(f"Number of orphans: {orphans.count()}")
        for orphan in orphans:
            path = self.get_node_path(orphan)
            Gphoto.objects(gid=orphan.gid).update_one(upsert=True, path=path)
        self.log.info(f"Cache stats: {self.get_node_path.cache_info()}")

    @functools.lru_cache()
    def get_node_path(self, node):
        assert len(node.parents) > 0, "Got node with no parents"
        if node.parents[0] == self.root.gid:
            return []
        try:
            parent = Gphoto.objects(gid=node.parents[0]).get()
        except me.MultipleObjectsReturned as e:
            self.log.warning(
                f"Wrong number of records returned for {node.gid}. Error {e}"
            )
            return ["*MultiParents*"]
        except me.DoesNotExist as e:
            self.log.warning(f"Parent does not exist. Error {e}")
            return ["*ParentNotInDb*"]
        if parent.path:
            return parent.path + [parent.name]
        else:
            return self.get_node_path(parent) + [parent.name]

    # def list_roots(self, names=None):
    #     root_id = service.files().get(fileId='root').execute().get('id')
    #     assert root_id != None, 'No root ID found for Google Drive'
    #     # query = f"parents in '{root_id}' and trashed = false and ( name = '" + "' or name = '".join(names) + "')"
    #     # nodes_json = service.files().list(q=query, fields=INIT_FIELDS).execute()
    #     # sterile_nodes = [self.steralize(x) for x in nodes_json['files']]
    #     for name in names:
    #         query = f"parents in '{root_id}' and trashed = false and name = '{name}'"
    #         node_json = self.steralize(service.files().list(q=query, fields=INIT_FIELDS).execute())
    #
    #     return Gphoto(**node_json)

    def get_node(self, id):
        # TODO: Looks like it needs to be the file name ('My Laptop') without parents
        node_json = (
            service.files().get(fileId=id, fields=FILE_FIELDS).execute()
        )  # TODO: Make sure search for not deleted nodes (right below root??)
        return Gphoto(**self.steralize(node_json))

    def steralize(self, node):
        if "id" in node:  # Mongoengine reserves 'id'
            node["gid"] = node.pop("id")
        if "size" in node:  # Mongoengine reserves 'size'
            node["gsize"] = node.pop("size")
        if "kind" in node:
            del node["kind"]
        return node

    def database_clean(self, set_state=None):
        if set_state is None:
            try:
                db_clean = Gphoto_state.objects().get().database_clean
            except me.MultipleObjectsReturned:
                raise me.MultipleObjectsReturned(
                    "State database has more than one record - should never happen."
                )
            except me.DoesNotExist:
                return self.database_clean(False)
            return db_clean
        else:
            assert isinstance(set_state, bool), "State must be boolean."
            Gphoto_state.objects().update_one(upsert=True, database_clean=set_state)
            return set_state


if __name__ == "__main__":
    main()