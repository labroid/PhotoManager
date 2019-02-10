import glob
import time
import os
import os.path
import shutil
import datetime
from loguru import logger
from pathlib import Path

import mongoengine as me
from me_models import Db_connect, Queue, State, Gphoto, SourceList

from utils import file_md5sum, config
from gphoto_upload import upload_to_gphotos
from drive_walk import GphotoSync

cfg = config()
logger.add("app.log", rotation="1 MB")
Db_connect()

# TODO: Change logging levels to eliminate most logging


def main():
    QueueWorker()
    print("Main Done")
    """
    Queue maintenance runs continuously. 
    """


class QueueWorker:
    def __init__(self):
        State.drop_collection()
        self.state = State(
            purge_ok=cfg.settings.purge_ok,
            mirror_ok=cfg.settings.mirror_ok,
            mirror_root=cfg.local.mirror_root,
        ).save()

        self.gphoto_sync = GphotoSync()
        Queue.drop_collection()  # TODO: Consider commenting out - or allow option to repopulate
        self.process_queue()

    def process_queue(self):
        while True:
            self.gphoto_sync.sync()
            self.add_candidates()
            self.update_md5s()
            self.check_gphotos_membership()
            self.upload_missing_media()
            self.dequeue()
            if not Queue.objects(in_gphotos=False):
                print("Waiting...")
                time.sleep(5)

    def add_candidates(self):
        self.state.reload()
        if self.state.target == self.state.old_target:
            return
        self.state.modify(old_target=self.state.target)
        logger.info("Start processing target dir(s)...")
        dirsize = 0

        start = datetime.datetime.now()
        self.state.modify(dirlist=list(glob.iglob(self.state.target)))
        logger.info(f"Target list: {self.state.dirlist}")
        for top in self.state.dirlist:
            logger.info(f"Traversing tree at {top} and adding to queue.")
            top_path = Path(top)
            for path in top_path.rglob("**/*"):
                ext = path.suffix.lower()
                if ext in cfg.local.image_filetypes:
                    size = path.stat().st_size
                    dirsize += size
                    Queue(src_path=str(path), size=size).save()
                else:
                    ext = ext.replace(
                        ".", ""
                    )  # Database can't handle keys starting with dot
                    excluded = self.state.excluded_ext_dict
                    if ext in excluded:
                        excluded[ext] += 1
                    else:
                        excluded[ext] = 1
                    self.state.update(excluded_ext_dict=excluded)
        self.state.save()
        elapsed = datetime.datetime.now() - start
        self.state.modify(
            dirsize=self.state.dirsize + dirsize,
            dirtime=elapsed.seconds + elapsed.microseconds / 1e6,
        )
        return

    # noinspection PyMethodMayBeStatic
    def update_md5s(self):
        for photo in Queue.objects(md5sum=None):
            photo.modify(md5sum=file_md5sum(photo.src_path))
        logger.info("MD5 Done")

    # noinspection PyMethodMayBeStatic
    def check_gphotos_membership(self):
        for photo in Queue.objects(me.Q(md5sum__ne=None) & me.Q(in_gphotos=False)):
            match = Gphoto.objects(md5Checksum=photo.md5sum).first()
            if match:
                photo.gphotos_path = match.path
                photo.gid = match.gid
                photo.in_gphotos = True
                photo.original_filename = match.originalFilename
            else:
                photo.in_gphotos = False
            photo.save()
        logger.info("Check Gphotos enqueue done")

    # noinspection PyMethodMayBeStatic
    def upload_missing_media(self):
        upload_candidate = Queue.objects(in_gphotos=False).first()
        if upload_candidate:
            upload_to_gphotos(upload_candidate.src_path)

    def mirror_file(self, photo):
        dest = Path(cfg.local.mirror_root, *photo.gphotos_path, photo.original_filename)
        if not dest.is_file():
            self.copy_file(photo=photo, dest=dest)
            logger.info(f"Mirrored {photo.src_path} to {dest}")
        else:
            if file_md5sum(dest) == file_md5sum(photo.src_path):
                self.copy_file(photo=photo, dest=None)
                logger.info(f"Already mirrored: {photo.src_path}")
            else:
                name = Path(photo.original_filename)
                new_filename = name.stem + photo.gid[-4:] + name.suffix
                dest = dest.parent / new_filename
                self.copy_file(photo=photo, dest=dest)
                logger.info(f"Mirrored {photo.src_path} to {dest}")
        photo.update(mirrored=True)

    # noinspection PyMethodMayBeStatic
    def copy_file(self, photo, dest=None):
        if dest:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(photo.src_path, dest)
        try:
            sources = SourceList.objects(md5sum=photo.md5sum).get()
        except me.DoesNotExist:
            SourceList(md5sum=photo.md5sum, paths=[photo.src_path]).save()
        else:
            sources.update(add_to_set__paths=[photo.src_path])

    def dequeue(self):
        for photo in Queue.objects(in_gphotos=True):
            if self.state.mirror_ok:
                if not photo.mirrored:
                    self.mirror_file(photo)
            if (
                self.state.purge_ok
                and photo.src_path
                and os.path.isfile(photo.src_path)
            ):
                print(f"This is where we would delete from source {photo.src_path}")
                #    os.remove(photo.src_path)
                photo.update(purged=True)


if __name__ == "__main__":
    main()
