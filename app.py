import datetime
import glob
import hashlib
import io
import os
import os.path
import shutil
import time
from pathlib import Path
import json

import mongoengine as me
from PIL import Image
from image_match.goldberg import ImageSignature
from loguru import logger

# from gphoto_upload import upload_to_gphotos
from me_models import DbConnect, Queue, State, Gphoto, SourceArchive
from utils import file_md5sum, config
from firebase import get_firestore_db

cfg = config()
logger.add("app.log", rotation="1 MB")
DbConnect()

db = get_firestore_db()
photos = db.collection('local_photos')

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

        # while True:
        #     response = input("Clear upload Queue? (y/n)")
        #     if response == "y":
        #         Queue.drop_collection()
        #         logger.info("********** Starting new run with cleared queue **********")
        #         break
        #     if response == "n":
        #         logger.info("********** Starting new run with existing queue **********")
        #         break
        self.process_queue()

    def process_queue(self):
        while True:
            self.add_candidates()
            # self.check_gphotos_membership()
            # self.upload_missing_media()
            # self.dequeue()
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

    def get_bytes(self, path):
        with open(path, mode="rb") as fp:
            return fp.read()

    def add_candidates(self):
        if not self.new_target_list():
            return ()
        dirsize = 0
        start = datetime.datetime.now()
        logger.info(f"Walking target list: {self.state.dirlist}")
        gis = ImageSignature()
        for top in self.state.dirlist:
            message = f"Traversing tree at {top} and adding to queue."
            logger.info(message)
            self.status(message)
            top_path = Path(top)
            for path in top_path.rglob("**/*"):
                ext = path.suffix.lower()
                if ext in cfg.settings.image_filetypes:
                    size = path.stat().st_size
                    dirsize += size
                    photo_b = self.get_bytes(path)
                    md5sum = hashlib.md5(photo_b).hexdigest()
                    # if not MD%sum already in database:
                    im = Image.open(io.BytesIO(photo_b))
                    tags = {
                        "cameraMake": im.info['parsed_exif'].get(0x010f, ""),
                        "cameraModel": im.info['parsed_exif'].get(0x0110, ""),
                        "creationTime": im.info['parsed_exif'].get(0x9003, ""),
                        "width": im.width,
                        "height": im.height,
                    }
                    image_md5 = hashlib.md5(im.tobytes()).hexdigest()
                    signature = gis.generate_signature(
                        photo_b, bytestream=True
                    ).tolist()
                    record = {
                        "src_path": str(path),
                        "size": size,
                        "md5sum": md5sum,
                        "image_md5": image_md5,
                        "signature": signature,
                        "mediaMetadata": tags,
                    }
                    photos.add(record)
                    logger.info(f"Added: {path}")
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
        logger.info(
            f"In gphotos: {Queue.objects(in_gphotos=True).count()}, Not in gphotos: {Queue.objects(in_gphotos=False).count()}"
        )

    # # noinspection PyMethodMayBeStatic
    # def upload_missing_media(self):
    #     if not self.state.upload_ok:
    #         return
    #     upload_candidate = Queue.objects(
    #         me.Q(in_gphotos=False) & me.Q(uploading=False) & me.Q(uploaded=False)
    #     ).first()
    #     if upload_candidate:
    #         message = f"Upload candidate: {upload_candidate.src_path}"
    #         print(message)
    #         self.status(message)
    #         tries = upload_candidate.upload_tries + 1
    #         upload_candidate.modify(uploading=True)
    #         success, elapsed = upload_to_gphotos(upload_candidate.src_path)
    #         if success:
    #             upload_candidate.modify(
    #                 uploading=False,
    #                 uploaded=True,
    #                 upload_tries=tries,
    #                 upload_elapsed=elapsed,
    #             )
    #         else:
    #             upload_candidate.modify(
    #                 uploading=False,
    #                 uploaded=False,
    #                 upload_tries=tries,
    #                 upload_elapsed=elapsed,
    #             )

    def mirror_file(self, photo):
        dest = Path(cfg.local.mirror_root, *photo.gphotos_path, photo.original_filename)
        if not dest.is_file():
            self.copy_file(photo=photo, dest=dest)
            return
        if file_md5sum(dest) == file_md5sum(photo.src_path):
            logger.info(f"Already mirrored: {photo.src_path}")
            if self.state.purge_ok:
                os.remove(photo.src_path)
                photo.update(purged=True)
            photo.modify(mirrored=True)
            return
        name = Path(photo.original_filename)
        new_filename = name.stem + photo.gid[-4:] + name.suffix
        dest = dest.parent / new_filename
        self.copy_file(photo=photo, dest=dest)

    # noinspection PyMethodMayBeStatic
    def copy_file(self, photo, dest):
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        if self.state.purge_ok:
            shutil.move(photo.src_path, dest)
            photo.update(purged=True)
        else:
            shutil.copy2(photo.src_path, dest)
        photo.update(mirrored=True)
        logger.info(f"Mirrored {photo.src_path} to {dest}")

    def dequeue(self):
        if self.state.mirror_ok:
            for photo in Queue.objects(me.Q(in_gphotos=True) & me.Q(mirrored=False)):
                self.mirror_file(photo)
        if self.state.purge_ok:
            for photo in Queue.objects(
                me.Q(in_gphotos=True) & me.Q(purged=False) & me.Q(mirrored=True)
            ):
                if photo.src_path and os.path.isfile(photo.src_path):
                    logger.info(f"Purge: {photo.src_path}")
                    os.remove(photo.src_path)
                    photo.update(purged=True)

    # def dequeue(self):
    #     if self.state.mirror_ok and self.state.purge_ok:
    #         pass #move file and update database
    #     elif self.state.mirror_ok and not self.state.purge_ok:
    #         pass #copy file and update database
    #     elif not self.state.mirror_ok and self.state.purge_ok:
    #         pass #delete w/o copying

    def status(self, status):
        temp = self.state.status
        temp.pop(0)
        temp.append(status + "\r\n")
        self.state.update(status=temp)


if __name__ == "__main__":
    main()
