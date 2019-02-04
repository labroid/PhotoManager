import glob
import time
import os
import os.path
import shutil
import datetime
from loguru import logger

import mongoengine as me
from me_models import Db_connect, Queue, State, Gphoto, Gphoto_parent

from utils import file_md5sum, config
from gphoto_upload import upload_to_gphotos
from drive_walk import GphotoSync

cfg = config()
logger.add("app.log", rotation="1 MB")
Db_connect()

# TODO: Save source path in permanent database for future naming
# TODO: Consider changing over to pathlib
# TODO: Change logging levels to elmiinate most logging


def main():
    QueueWorker()
    print("Main Done")
    """
    Queue maintenance runs continuously. Analyze queue before adding new files (since we need to know if files
    are already in the queue as well as already in gphotos to know if we want to add them to the queue).

    drop queue db (in future make durable and check)
    for each file in gphotos queue directory
        put file stats in queue db and mark as in queue
    while True: (async loop?)
        for each file in queue db
            update missing MD5 sums
            update missing gphoto membership
            mirror files already in gphotos and not mirrored
            optionally purge files already in gphotos if source still avalable
            remove files in gphotos from queue and mark done in db
        check for new files to be added (separate process or database for selecting and adding?)
            Get dir list from user and add to candidates
            Update missing MD5 sums for candidates
            update missing gphoto membership for queue
            if not in gphotos and not in gphoto queue add to gphoto queue
            mirror files already in gphotos and not mirrored
            Add upload candidates to gphoto queue.  Assure no name collision by unique directory name.

    *******Async try1*******

    drop queue db (TODO: make durable and check)
    for each file in gphotos queue directory
        put file stats in queue db and mark as in queue

    async process_files()
        get next file not in process
        mark db as in process
        await purge_file()

    async purge_file():
        await mirror_file()
        if purge_enabled:
            delete file

    async mirror_file():
        await file_in_gphotos()
        if mirror_enabled:
            copy to mirror()

    async file_in_gphotos():
        await get_md5()
        await check_gphotos()

*******Async try 2########
    drop queue db (in future make durable and check)
    drop candidates db
    for each file in gphotos queue directory
        put file stats in queue db and mark as in queue

    async queue_worker():
        while True:
            for each unprocessed file in queue db
                mark file as processing
                await update missing MD5 sums(_id list)
                await update missing gphoto membership(md5 list)
                await mirror files already in gphotos and not mirrored
                await optionally purge files already in gphotos if source still avalable
                await remove files in gphotos from queue and mark done in db

    async candidate_worker():
        while True:
            check for new files to be added (separate process or database for selecting and adding?)
                await Get dir list from user and add file stats to candidate
                await Update missing MD5 sums for candidates
                await update missing gphoto membership for candidates
                await if not in gphotos and not in gphoto queue add to gphoto queue
                await mirror files already in gphotos and not mirrored
                await Add upload candidates to gphoto queue.  Assure no name collision by unique directory name.
                await optionally purge files in gphotos if source still available
                await remove files from candidates that are mirrored and in gphotos

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
        # Queue.drop_collection()
        self.process_queue()

    def process_queue(self):
        while True:
            self.gphoto_sync.sync()
            self.add_candidates()
            self.update_md5s()
            self.check_gphotos_membership()
            self.upload_missing_media()
            self.dequeue()
            print("Waiting...")
            # os.sys.exit(1)
            time.sleep(5)

    def add_candidates(self):
        self.state.reload()
        if self.state.target == self.state.old_target:
            return
        self.state.modify(old_target=self.state.target)
        logger.info("Start processing target dir(s)...")
        dirsize = 0
        photos = {}

        start = datetime.datetime.now()
        self.state.modify(dirlist=list(glob.iglob(self.state.target)))
        logger.info(f"Target list: {self.state.dirlist}")
        for top in self.state.dirlist:
            logger.info(f"Traversing tree at {top} and adding to queue.")
            for root, dirs, files in os.walk(
                top
            ):  # TODO:  Add error trapping argument and function
                for path in [os.path.join(root, filename) for filename in files]:
                    file_ext = os.path.splitext(path)[1].lower()
                    if file_ext in cfg.local.image_filetypes:
                        size = os.stat(path).st_size
                        dirsize += size
                        Queue(src_path=path, size=size).save()
                    else:
                        ext = file_ext.replace('.', '')  # Because database can't take dict indices starting with .
                        excluded = self.state.excluded_ext_dict
                        if file_ext in excluded:
                            excluded[ext] += 1
                        else:
                            excluded[ext] = 1
                        self.state.update(excluded_ext_dict=excluded)
        self.state.save()
        # if self.state.excluded_ext_dict:
        #     excluded_list = [
        #         (str(k).replace(".", "") + "(" + str(v) + ")")
        #         for k, v in excluded_exts.items()
        #     ]
        # else:
        #     excluded_list = ["None"]
        elapsed = datetime.datetime.now() - start
        self.state.modify(
            dirsize=self.state.dirsize + dirsize,
            dirtime=elapsed.seconds + elapsed.microseconds/1e6,
        )
        return

    def update_md5s(self):
        for photo in Queue.objects(md5sum=None):
            photo.modify(md5sum=file_md5sum(photo.src_path))
        logger.info("MD5 Done")

    def check_gphotos_membership(self):
        for photo in Queue.objects(me.Q(md5sum__ne=None) & me.Q(in_gphotos=False)):
            match = Gphoto.objects(md5Checksum=photo.md5sum).first()
            if match:
                photo.gphotos_path = match.path
                photo.gid = match.gid
                photo.in_gphotos = True
            else:
                photo.in_gphotos = False
            photo.save()
        logger.info("Check Gphotos enqueue done")

    def upload_missing_media(self):
        upload_candidate = Queue.objects(in_gphotos=False).first()
        if upload_candidate:
            upload_to_gphotos(upload_candidate.src_path)

    def mirror_file(self, photo):
        dest_path = os.path.join(self.state.mirror_root, *photo.gphotos_path, photo.gid)
        if not os.path.isfile(dest_path):
            os.makedirs(os.path.dirname(dest_path), exist_ok=True)
            shutil.copy2(photo.src_path, dest_path)
            photo.update(mirrored=True)
            logger.info("Mirrored {} to {}".format(photo.src_path, dest_path))

    def dequeue(self):
        for photo in Queue.objects(in_gphotos=True):
            if self.state.mirror_ok:
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