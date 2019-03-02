import utils
import requests
import json
import os.path
from pprint import pformat
from oauth2creds import get_credentials
from loguru import logger

cfg = utils.config()
logger.add("gphoto_upload.log", rotation="1 MB")


def upload_to_gphotos(filepath, filename=None):
    if filename is None:
        filename = os.path.basename(filepath)
    response = _upload_binary_media(filepath, filename)
    if not response.ok:
        success = False
        elapsed = response.elapsed.microseconds/1_000_000

    else:
        success, elapsed = _insert_new_photo(response.text)
    return success, elapsed

def _upload_binary_media(filepath, filename):
    creds = get_credentials()
    with open(filepath, "rb") as photo_fp:
        binary_file = photo_fp.read()
    url = r"https://photoslibrary.googleapis.com/v1/uploads"
    headers = {
        "Content-type": "application/octet-stream",
        "Authorization": f"Bearer {creds.token}",
        "X-Goog-Upload-File-Name": f"{filename}",
        "X-Goog-Upload-Protocol": "raw",
    }
    r = requests.post(url, headers=headers, data=binary_file)
    if r.ok:
        log_status = "Upload successful"
    else:
        log_status = "Upload failed"
    logger.info(f"{log_status}: Upload elapsed time: {r.elapsed}")
    return r


def _insert_new_photo(token):
    creds = get_credentials()
    headers = {"Authorization": f"Bearer {creds.token}"}
    insert_new_media_item = {
        "newMediaItems": [{"simpleMediaItem": {"uploadToken": token}}]
    }
    url = r"https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate"
    r = requests.post(url=url, headers=headers, data=json.dumps(insert_new_media_item))
    response = r.json()
    status = response["newMediaItemResults"][0]["status"]["message"]
    if status != "OK":
        logger.info(f"NewMediaItem insertion failed. {pformat(response)}")
        success = False
    else:
        logger.info(f"Insertion successful. {pformat(response)}")
        success = True
    elapsed = r.elapsed.microseconds/1_000_000
    print(f"Media insertion elapsed time: {r.elapsed.microseconds/1_000_000} seconds")
    return success, elapsed


if __name__ == "__main__":
    upload_to_gphotos(r"C:\Users\SJackson\Pictures\FZ80\P1000127.JPG")

    """
    POST https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate
Content-type: application/json
Authorization: Bearer OAUTH2_TOKEN
REQUEST_BODY

{
  "albumId": "ALBUM_ID",
  "newMediaItems": [
    {
      "description": "ITEM_DESCRIPTION",
      "simpleMediaItem": {
        "uploadToken": "UPLOAD_TOKEN"
      }
    }
    , ...
  ],
  "albumPosition": {
    "position": "AFTER_MEDIA_ITEM",
    "relativeMediaItemId": "MEDIA_ITEM_ID"
  }
}
====================================================
{
  "newMediaItemResult": [
    {
      "uploadToken": "UPLOAD_TOKEN",
      "status": {
        "code": "0"
      },
      "mediaItem": {
        "id": "MEDIA_ITEM_ID"
        "productUrl": "https://photos.google.com/photo/PHOTO_PATH",
        "description": "ITEM_DESCRIPTION"
        "baseUrl": "BASE_URL-DO_NOT_USE_DIRECTLY"
        "mediaMetadata": {
          "width": "MEDIA_WIDTH_IN_PX",
          "height": "MEDIA_HEIGHT_IN_PX"
          "creationTime": "CREATION_TIME",
          "photo": {},
        },
      }
    },
    {
      "uploadToken": "UPLOAD_TOKEN"
      "status": {
        "code": 13,
        "message": "Internal error"
      },
    }
  ]
}"""
