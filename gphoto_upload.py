from apiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools
import logging
from logging.config import dictConfig
from utils import Config
import requests
import json
import os.path
import argparse

cfg = Config()
dictConfig(cfg.logging)
log = logging.getLogger(__name__)


# TODO: This should be a little more mature: check return codes from the insert and make sure all photos made it up
def upload_to_gphotos(filepath, filename=None):
    if filename is None:
        filename = os.path.basename(filepath)
    response = upload_binary_media(filepath, filename)
    if response.ok:
        insert_new_photo(response.text)


def get_creds():
    SCOPES = 'https://www.googleapis.com/auth/photoslibrary'
    store = file.Storage('credentials.json')  # TODO: Put this in common dir
    creds = store.get()
    if not creds or creds.invalid or creds.access_token_expired:
        flow = client.flow_from_clientsecrets('client_secrets_web.json', SCOPES)  # TODO: Put this in common dir
        creds = tools.run_flow(flow, store)
    return creds


def upload_binary_media(filepath, filename):
    creds = get_creds()
    with open(filepath, 'rb') as photo_fp:
        binary_file = photo_fp.read()
    url = r"https://photoslibrary.googleapis.com/v1/uploads"
    headers = {
        "Content-type": "application/octet-stream",
        "Authorization": f"Bearer {creds.access_token}",
        "X-Goog-Upload-File-Name": f"{filename}",
        "X-Goog-Upload-Protocol": "raw",
    }
    r = requests.post(url, headers=headers, data=binary_file)
    if r.ok:
        log_status = "Upload successful"
    else:
        log_status = "Upload failed"
    log.info(f"{log_status}: Upload elapsed time: {r.elapsed}")
    return r


def insert_new_photo(token):
    creds = get_creds()
    headers = {"Authorization": f"Bearer {creds.access_token}"}
    insert_new_media_item = {
        "newMediaItems": [
            {
                "simpleMediaItem": {
                    "uploadToken": token
                }
            }
        ]
    }
    url = r"https://photoslibrary.googleapis.com/v1/mediaItems:batchCreate"
    r = requests.post(url=url, headers=headers, data=json.dumps(insert_new_media_item))
    response = r.json()
    status = response['newMediaItemResults'][0]['status']['message']
    if status != 'OK':
        log.info(f"NewMediaItem insertion failed. Code {status}. Token {token}")
    else:
        log.info(f"Insertion successful token {token}")
    print(f"Media insertion elapsed time: {r.elapsed.microseconds/1000000} seconds")
    # TODO: Put response 'r' into the database


if __name__ == "__main__":
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--noauth_local_webserver', action='store_true') # I hope this doesn't break the setting....
    # parser.add_argument('--name', dest='filename', default=None, help="filename if different than path basename")
    # parser.add_argument('path', help='path to photo you want to upload')
    # args = parser.parse_args()

    # upload_to_gphotos(args.path, args.filename)
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
