from hashlib import md5
import io
from dataclasses import dataclass, asdict
from pathlib import Path
import json

import PIL
from PIL import ImageFile
import exif
from PIL import Image
from loguru import logger

from utils import file_md5sum

# TODO:  Add HEIC tag support. exif-py package maybe??
# TODO:  Filter file types for exif extraction - png for example doesn't have useful metadata


@dataclass
class ImageMetadata:
    path: str = None
    size: int = 0
    md5sum: str = ''
    image_md5: str = ''
    camera_make: str = ''
    camera_model: str = ''
    creation_time: str = ''
    datetime_original: str = ''
    width: int = 0
    height: int = 0


ImageFile.LOAD_TRUNCATED_IMAGES = True


def photo_file_metadata(filepath: Path) -> ImageMetadata:  # TODO:  Make accept path_string, file_pointer, or Path
    """
    Extract metadata from file path.
    :param filepath:
    :return ImageMetadata:  TODO: Finish me
    """

    result = ImageMetadata(path=str(filepath), size=filepath.stat().st_size)
    if result.size > 100e6:
        result.md5sum = file_md5sum(str(filepath))
        return result
    photo_b = filepath.read_bytes()
    result.md5sum = md5(photo_b).hexdigest()
    if filepath.suffix.lower() in ['.mov', '.mp4', '.avi', '.json', '.rw2']:  #  TODO: Look at results and refine list
        return result
    try:
        im = PIL.Image.open(io.BytesIO(photo_b))
    except FileNotFoundError:
        logger.critical(f"{filepath} not found")
        return result
    except (PIL.UnidentifiedImageError, ValueError, TypeError) as e:
        logger.warning(f"{filepath} is a type PIL does not recognize: {e}")
        return result
    result.width = im.width
    result.height = im.height
    result.image_md5 = md5(im.tobytes()).hexdigest()
    extract_exif_data(filepath, photo_b, result)
    return result


def extract_exif_data(filepath, photo_b, result):
    try:
        exif_data = exif.Image(photo_b)
    except Exception as e:  # TODO: Refine this broad except
        logger.warning(f"{filepath} extracting exif data had unexpected error: {e}")
        return result
    if exif_data.has_exif:
        try:
            result.camera_make = exif_data.get('make', None)
            if result.camera_make and (result.camera_make[-3] == '\x00'):
                result.camera_make = result.camera_make[0:30].rstrip('\x00')
        except Exception as e:
            logger.warning(f"{filepath} extracting camera make had unexpected error: {e}")
        try:
            result.camera_model = exif_data.get('model', None)
            if result.camera_model and (result.camera_model[-3] == '\x00'):
                result.camera_model = result.camera_model[0:30].rstrip('\x00')
        except Exception as e:
            logger.warning(f"{filepath} extracting camera model had unexpected error: {e}")
        try:
            result.creation_time = exif_data.get('datetime', None)
        except Exception as e:
            logger.warning(f"{filepath} extracting datatime had unexpected error: {e}")
        try:
            result.datetime_original = exif_data.get('datetime_original', None)
        except Exception as e:
            logger.warning(f"{filepath} extracting datetime_oringinal had unexpected error: {e}")
    return result


if __name__ == "__main__":
    d = photo_file_metadata(Path(r"F:\Gphotos Mirror\Google Photos\February 2016\20160220_172625-edited.jpg"))
    print(f"Object: {d}")
    print(f"Dict: {asdict(d)}")
    print(f"JSON: {json.dumps(asdict(d))}")
