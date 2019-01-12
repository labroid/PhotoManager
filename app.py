import collections
import glob
import logging
import os
import os.path
import shutil
import time
from logging.config import dictConfig

import mongoengine as me

from models import Db_connect, Queue, State, Gphoto, Gphoto_parent
from utils import file_md5sum, Config

cfg = Config()
dictConfig(cfg.logging)
log = logging.getLogger(__name__)  # TODO:  Logging not correctly configured
Db_connect()


def main():
    initialize_state()
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


def initialize_state():  # TODO: Consider putting state defaults in the config file
    State.drop_collection()
    state = State()
    state.dirlist = [""]
    state.purge_ok = True
    state.mirror_ok = True
    state.old_target = None
    state.target = r"C:\Users\SJackson\Pictures\Amy Plants"
    state.save()


class QueueWorker:
    def __init__(self):
        self.photo_queue = cfg.local.gphoto_upload_queue
        self.mirror = cfg.local.mirror_root
        self.sync_db_to_queue()
        self.process_queue()

    def sync_db_to_queue(self):
        print("Creating db synced to queue")
        Queue.drop_collection()  # TODO:  Make more efficient by not dropping but checking size/mtime for changes
        for root, dirs, files in os.walk(
            self.photo_queue, topdown=True
        ):  # TODO:  Consider trapping failures
            for path in [os.path.join(root, filename) for filename in files]:
                photo = Queue()
                photo.queue_path = path
                photo.src_path = (
                    None
                )  # Don't know source for things already in the queue
                photo.queue_state = "enqueued"  # By definition
                photo.size = os.stat(path).st_size
                photo.save()
        print("Done syncing db to queue")

    def process_queue(self):  # TODO:  Make this async
        while True:
            self.update_md5s()
            self.check_gphotos_enqueue()
            self.dequeue()
            self.candidates()
            print("Waiting...")
            os.sys.exit(1)
            time.sleep(5)

    #         self.enqueue()

    def update_md5s(self):
        for photo in Queue.objects(md5sum=None):
            photo.md5sum = file_md5sum(photo.queue_path or photo.src_path)
            photo.save()
        # print("MD5 Done")
        log.info("MD5 Done")

    def check_gphotos_enqueue(self):
        photos = Queue.objects(me.Q(md5sum__ne=None) & me.Q(in_gphotos=False))
        md5list = [photo.md5sum for photo in photos]
        results = Gphoto.objects(md5Checksum__in=md5list).scalar(
            "md5Checksum", "parents", "originalFilename"
        )  # list(results) is a list. results[0][1][0] is parent gid, results[0][0] is the MD5
        md5_to_parentgid = {result[0]: result[1][0] for result in results}
        md5_to_origfilename = {result[0]: result[2] for result in results}
        gphoto_parents = Gphoto_parent.objects(gid__in=set(md5_to_parentgid.values()))
        paths = {parent.gid: os.path.join(*parent.path) for parent in gphoto_parents}
        timestamp = time.strftime("%Y%m%d%H%M%S")
        for photo in photos:
            if photo.md5sum in md5_to_parentgid:
                photo.gphotos_path = paths[md5_to_parentgid[photo.md5sum]]
                photo.original_filename = md5_to_origfilename[photo.md5sum]
                photo.in_gphotos = True
            else:
                photo.in_gphotos = False
                if (
                    photo.src_path
                ):  # There is no src_path if photo was already in the queue
                    dest_path = os.path.join(
                        self.photo_queue, timestamp, os.path.basename(photo.src_path)
                    )
                    if photo.queue_state is None and not os.path.isfile(dest_path):
                        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
                        shutil.copy2(photo.src_path, dest_path)
                        photo.queue_path = dest_path
                        photo.queue_state = "enqueued"
            photo.save()
        log.info("Check Gphotos done")

    def mirror_file(self, photo):  # TODO: Should store to Gphotos ID, not file name
        src_path = photo.queue_path or photo.src_path
        dest_dir = photo.gphotos_path
        dest_path = os.path.join(self.mirror, dest_dir, photo.original_filename)
        if not os.path.isfile(dest_path):
            os.makedirs(dest_dir, exist_ok=True)
            shutil.copy2(src_path, dest_path)
            log.info("Mirrored {} to {}".format(src_path, dest_path))

    def dequeue(self):
        for photo in Queue.objects(
            me.Q(in_gphotos=True) & me.Q(queue_state__ne="done")
        ):
            if State.objects.get().mirror_ok:
                self.mirror_file(photo)
            if os.path.isfile(photo.queue_path):
                print(
                    "This is where we would delete from queue {}".format(
                        photo.queue_path
                    )
                )
                # os.remove(photo.queue_path)
            if (
                State.objects.get().purge_ok
                and photo.src_path
                and os.path.isfile(photo.src_path)
            ):
                print(
                    "This is where we would delete from source {}".format(
                        photo.queue_path
                    )
                )
            #    os.remove(photo.src_path)
            photo.queue_state = "done"
            photo.save()

    def candidates(self):
        target = State.objects.get().target
        old_target = State.objects.get().old_target
        if target != old_target:
            State.objects().update_one(old_target=target)
        candidates = self.scan_dirs(target)
        for path, size in candidates.items():
            photo = Queue()
            photo.src_path = path
            photo.size = size
            # photo.queue_state = None  #Defaults to this state
            # photo.in_gphotos = False  #Defaults to this state
            photo.save()

    def scan_dirs(self, target):
        log.info("Start processing dir")
        dirsize = 0
        photos = {}
        excluded_exts = collections.defaultdict(int)

        start = time.time()
        target_list = list(glob.iglob(target))
        log.info("target list: {}".format(target_list))
        for top in target_list:
            log.info("Traversing tree at {} and storing paths.".format(top))
            for root, dirs, files in os.walk(
                top
            ):  # TODO:  Add error trapping argument and function
                for path in [os.path.join(root, filename) for filename in files]:
                    file_ext = os.path.splitext(path)[1].lower()
                    if file_ext in cfg.local.image_filetypes:
                        size = os.stat(path).st_size
                        photos[path] = size
                        dirsize += size
                    else:
                        excluded_exts[os.path.splitext(path)[1].lower()] += 1
        if len(excluded_exts):
            excluded_list = [
                (str(k).replace(".", "") + "(" + str(v) + ")")
                for k, v in excluded_exts.items()
            ]
        else:
            excluded_list = ["None"]
        State.objects().update_one(
            dirlist=target_list,
            dirfilecount=len(photos),
            dir_excluded_list=excluded_list,
            dirsize=dirsize,
            dirtime=time.time() - start,
        )
        return photos

    # def enqueue(self, photo):  #TODO: This is old and must be updated *************
    #
    #     self.update_md5s(Candidates)  #TODO:  Stopped here
    #     dest = os.path.join(self.photo_queue.root_path, os.path.splitdrive(photo.path)[1])
    #     if os.path.isfile(dest):  # Check for name collision
    #         path_parts = os.path.split(dest)
    #         dest = os.path.join(path_parts[0], 'duplicate_name', path_parts[1])
    #         if os.path.isfile(dest):  # If still have name collision after putting in dup name dir, give up
    #             logging.warning("Photo queue name {} exists; skipping".format(dest))
    #     logging.info(photo['path'], "not in gphotos, non-dup, putting in queue")
    #     shutil.copy2(photo.path, dest)
    #     photo.queue_state = 'enqueued'
    #     photo.save()


if __name__ == "__main__":
    main()
