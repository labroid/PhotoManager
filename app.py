import glob
import time
import os
import os.path
import shutil
import datetime

from loguru import logger
from pathlib import Path

import mongoengine as me
from me_models import DbConnect, Queue, State, Gphoto, SourceArchive, Candidates

from utils import file_md5sum, config
from gphoto_upload import upload_to_gphotos
from drive_walk import GphotoSync

cfg = config()
logger.add("app.log", rotation="1 MB")
DbConnect()

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
            status=["\r\n", "\r\n", "Initialized\r\n"],
        ).save()

        self.gphoto_sync = GphotoSync()
        while True:
            response = input("Clear upload Queue? (y/n)")
            if response == "y":
                Queue.drop_collection()
                logger.info("********** Starting new run with cleared queue **********")
                break
            if response == "n":
                logger.info("********** Starting new run with existing queue **********")
                break
        self.process_queue()

    def process_queue(self):
        while True:
            self.status("Syncing with Gphotos")
            self.gphoto_sync.sync()
            self.add_candidates()
            self.check_gphotos_membership()
            self.upload_missing_media()
            self.dequeue()
            print("Waiting...")
            time.sleep(5)
            # if not Queue.objects(in_gphotos=False):
            #     print("Waiting...")
            #     time.sleep(5)

    def new_target_list(self):
        self.state.reload()
        if self.state.target == self.state.old_target:
            return False
        self.state.modify(old_target=self.state.target)
        self.state.modify(dirlist=list(glob.iglob(self.state.target)))
        return True

    def add_candidates(self):
        if not self.new_target_list():
            return()
        dirsize = 0
        start = datetime.datetime.now()
        logger.info(f"Walking target list: {self.state.dirlist}")
        for top in self.state.dirlist:
            message = f"Traversing tree at {top} and adding to queue."
            logger.info(message)
            self.status(message)
            top_path = Path(top)
            for path in top_path.rglob("**/*"):
                ext = path.suffix.lower()
                if ext in cfg.local.image_filetypes:
                    size = path.stat().st_size
                    dirsize += size
                    md5sum = file_md5sum(path)
                    if not Queue.objects(md5sum=md5sum):
                        Queue(src_path=str(path), size=size, md5sum=md5sum).save()
                        logger.info(f"Enqueuing: {path}")
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
        self.status("Updating MD5s...")
        for photo in Queue.objects(md5sum=None):
            photo.modify(md5sum=file_md5sum(photo.src_path))
        logger.info("MD5 Done")

    # noinspection PyMethodMayBeStatic
    def check_gphotos_membership(self):
        self.status("Checking for photos not in Gphotos")
        for photo in Queue.objects(me.Q(md5sum__ne=None) & me.Q(in_gphotos__ne=True)):
            match = Gphoto.objects(md5Checksum=photo.md5sum).first()
            if match:
                photo.gphotos_path = match.path
                photo.gid = match.gid
                photo.in_gphotos = True
                photo.original_filename = match.originalFilename
                logger.info(f"In Gphotos: {photo.src_path}")
            else:
                if photo.in_process is False:
                    logger.info(f"Not in Gphotos: {photo.src_path}")
                photo.in_gphotos = False
            photo.in_process = True
            photo.save()
            try:
                sources = SourceArchive.objects(md5sum=photo.md5sum).get()
            except me.DoesNotExist:
                SourceArchive(md5sum=photo.md5sum, paths=[photo.src_path]).save()
            else:
                sources.update(add_to_set__paths=[photo.src_path])
        logger.info(f"In gphotos: {Queue.objects(in_gphotos=True).count()}, Not in gphotos: {Queue.objects(in_gphotos=False).count()}")

    # noinspection PyMethodMayBeStatic
    def upload_missing_media(self):
        if not self.state.upload_ok:
            return
        upload_candidate = Queue.objects(
            me.Q(in_gphotos=False) & me.Q(uploading=False) & me.Q(uploaded=False)
        ).first()
        if upload_candidate:
            message = f"Upload candidate: {upload_candidate.src_path}"
            print(message)
            self.status(message)
            tries = upload_candidate.upload_tries + 1
            upload_candidate.modify(uploading=True)
            success, elapsed = upload_to_gphotos(upload_candidate.src_path)
            if success:
                upload_candidate.modify(
                    uploading=False,
                    uploaded=True,
                    upload_tries=tries,
                    upload_elapsed=elapsed,
                )
            else:
                upload_candidate.modify(
                    uploading=False,
                    uploaded=False,
                    upload_tries=tries,
                    upload_elapsed=elapsed,
                )

    def mirror_file(self, photo):
        self.status("Mirroring files")
        dest = Path(cfg.local.mirror_root, *photo.gphotos_path, photo.original_filename)
        if not dest.is_file():
            self.copy_file(photo=photo, dest=dest)
            logger.info(f"Mirrored {photo.src_path} to {dest}")
        else:
            if file_md5sum(dest) == file_md5sum(photo.src_path):
                # self.copy_file(photo=photo, dest=None)
                logger.info(f"Already mirrored: {photo.src_path}")
            else:
                name = Path(photo.original_filename)
                new_filename = name.stem + photo.gid[-4:] + name.suffix
                dest = dest.parent / new_filename
                self.copy_file(photo=photo, dest=dest)
                logger.info(f"Mirrored {photo.src_path} to {dest}")
        photo.modify(mirrored=True)

    # noinspection PyMethodMayBeStatic
    def copy_file(self, photo, dest=None):
        if dest:
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            shutil.copy2(photo.src_path, dest)

    def dequeue(self):
        if self.state.mirror_ok:
            for photo in Queue.objects(me.Q(in_gphotos=True) & me.Q(mirrored=False)):
                self.mirror_file(photo)
        if self.state.purge_ok:
            for photo in Queue.objects(me.Q(in_gphotos=True) & me.Q(purged=False) & me.Q(mirrored=True)):
                if photo.src_path and os.path.isfile(photo.src_path):
                    logger.info(f"Purge: {photo.src_path}")
                    os.remove(photo.src_path)
                    photo.update(purged=True)

    def status(self, status):
        temp = self.state.status
        temp.pop(0)
        temp.append(status + "\r\n")
        self.state.update(status=temp)


if __name__ == "__main__":
    main()
