import hashlib
import io

from PIL import Image
from loguru import logger

from me_models import DbConnect, Queue, Src_metadata
from utils import config, file_md5sum

PARSED_EXIF = 'parsed_exif'

cfg = config()
logger.add("update_metadata.log", rotation="1 MB")
DbConnect()


def main():
    # TODO: Set state here if desired
    for photo in Queue.objects(md5sum=None):
        update_metadata(photo)


# noinspection PyMethodMayBeStatic
def get_bytes(path):
    with open(path, mode="rb") as fp:
        return fp.read()


def update_metadata(photo):
    logger.info(f"Getting metadata from: {photo.src_path}")
    if photo.size > 1e9:
        photo.md5sum = file_md5sum(photo.src_path)
        photo.save()
        return
    photo_b = get_bytes(photo.src_path)
    photo.md5sum = hashlib.md5(photo_b).hexdigest()
    try:
        im = Image.open(io.BytesIO(photo_b))
        if PARSED_EXIF in im.info:
            src_metadata = Src_metadata()
            src_metadata.cameraMake = im.info[PARSED_EXIF].get(0x010f, "")
            src_metadata.cameraModel = im.info[PARSED_EXIF].get(0x0110, "")
            src_metadata.creationTime = im.info[PARSED_EXIF].get(0x9003, "")
            src_metadata.width = im.width
            src_metadata.height = im.height
            photo.src_metadata = src_metadata
        photo.image_md5 = hashlib.md5(im.tobytes()).hexdigest()
    except OSError:
        pass
    photo.save()


if __name__ == "__main__":
    main()
