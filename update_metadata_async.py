import hashlib
import io
import multiprocessing as mp
from concurrent.futures import ProcessPoolExecutor, as_completed
import time
import subprocess
from pathlib import Path
import shlex

from PIL import Image, ImageFile
from loguru import logger
import arrow

from me_models import DbConnect, Src_metadata
from me_models import Queue as PhotoQueue
from utils import config, file_md5sum

PARSED_EXIF = "parsed_exif"

cfg = config()
logger.add("update_metadata.log", rotation="100 MB")
DbConnect()


def main():
    job_queue = mp.Manager().Queue()
    logger.info("Filling queue")
    n = 0
    for n, photo in enumerate(PhotoQueue.objects(md5sum=None)):
        job_queue.put(photo)

    if not n:
        logger.info("Done. Queue is empty - no files without MD5SUM")
        return

    logger.info(f"Done filling queue with {n + 1} items")
    with ProcessPoolExecutor(max_workers=3) as executor:
        futures = []
        for worker_num in range(3):
            futures.append(
                executor.submit(update_metadata, queue=job_queue, worker_num=worker_num)
            )
            logger.info(f"Worker {worker_num} started")
        logger.info("Launched all workers and am back in main")

        for x in as_completed(futures):
            logger.info(x)


def update_metadata(queue, worker_num):
    logger.info(f"Starting worker {worker_num}")
    ImageFile.LOAD_TRUNCATED_IMAGES = True
    while not queue.empty():
        photo = queue.get()
        logger.info(f"Worker {worker_num} getting metadata from: {photo.src_path}")
        if photo.size > 100e6 or Path(photo.src_path).suffix.lower() in ['.mov', '.mp4', '.avi']:
            print(f"Skipping {photo.src_path}")
            # logger.info("Using shell MD5")
            # md5_target = Path(photo.src_path)
            # wsl_target = PurePosixPath('/mnt/') / md5_target.drive[0].lower() / PurePosixPath(*md5_target.parts[1:])
            # command = f"wsl md5sum {str(wsl_target)}"
            # command_string = f'"{command}"'
            # commands = shlex.split(command)
            # md5_shell = subprocess.run([*commands], capture_output=True)
            # md5_shell = subprocess.run([command_string], capture_output=True)
            # md5_shell = subprocess.run(['FCIV', '-md5', str(md5_target)])
            # if md5_shell.returncode == 0:
            #     md5sum = md5_shell.stdout
            # try:
            #     md5 = file_md5sum(photo.src_path)
            #     photo.md5sum = md5
            # except Exception as e:
            #     logger.info(f"Skipping {photo.src_path} for failure: {e}")
            #     continue
            # with open(photo.src_path, 'rb') as f:
            #     photo.md5sum = hashlib.md5(f.read()).hexdigest()
            # photo.save()
            continue
        photo_b = Path(photo.src_path).read_bytes()
        photo.md5sum = hashlib.md5(photo_b).hexdigest()
        try:
            im = Image.open(io.BytesIO(photo_b))
        except OSError:
            logger.info(f"File {photo.src_path} won't process as image. Saving MD5 and moving on.")
            photo.save()
            continue
        if PARSED_EXIF in im.info:
            src_metadata = Src_metadata()
            src_metadata.cameraMake = im.info[PARSED_EXIF].get(0x010F, None)
            src_metadata.cameraModel = im.info[PARSED_EXIF].get(0x0110, None)
            creation_time = im.info[PARSED_EXIF].get(0x0132, None)
            if creation_time:
                src_metadata.creationTime = str_to_datetime(creation_time)
            datetime_original = im.info[PARSED_EXIF].get(0x9003, None)
            if datetime_original:
                src_metadata.dateTimeOriginal = str_to_datetime(datetime_original)
            src_metadata.width = im.width
            src_metadata.height = im.height
            photo.src_metadata = src_metadata
        photo.image_md5 = hashlib.md5(im.tobytes()).hexdigest()
        photo.save()


def str_to_datetime(datestring):
    if datestring:
        t = "".join(
            [x for x in datestring if x in "0123456789:/ "]
        )  # Strip extraneous characters and binary
    try:
        converted = arrow.get(
            t, ["YYYY:MM:DD HH:mm:ss", "YYYY:MM:DD HH:mm", "DD/MM/YYYY"]
        )
        return converted.datetime
    except ValueError as e:
        logger.warning(f"Value error: Datestring: {t} Binary equiv: {t.encode()} {e}")
        return None


if __name__ == "__main__":
    main()
