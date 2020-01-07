from me_models import DbConnect, Queue
from utils import config
from loguru import logger
import re
import arrow

PARSED_EXIF = "parsed_exif"

cfg = config()
logger.add("duplicate_analyzer_mongodb.log", rotation="100 MB")
DbConnect()

# pipeline = [
#     {
#         "$group": {
#             "_id": {"md5sum": "$md5sum"},
#             "uniqueIds": {"$addToSet": "$_id"},
#             "count": {"$sum": 1},
#          },
#     },
#     {
#         "$match": {"count": {"$gt": 1}}
#     },
#     {
#         "$sort": {"count": -1},
#     },
#     {
#         "$lookup": {
#             "from": "photo",
#             "localField": "uniqueIds",
#             "foreignField": "_id",
#             "as": "src_path",
#         },
#     },
# ]
# candidates = Queue.objects.aggregate(*pipeline)
# for c in candidates:
#     print('------------')
#     for path in c['src_path']:
#         print(path['src_path'])

# pipeline = [
#     {
#         "$group": {
#             "_id": {"image_md5": "$image_md5"},
#             "uniqueIds": {"$addToSet": "$_id"},
#             "count": {"$sum": 1},
#         },
#     },
#     {
#         "$match": {"count": {"$gt": 1}}
#     },
#     {
#         "$sort": {"count": -1},
#     },
#     {
#         "$lookup": {
#             "from": "photo",
#             "localField": "uniqueIds",
#             "foreignField": "_id",
#             "as": "src_path",
#         },
#     },
# ]
#
# candidates = Queue.objects.aggregate(*pipeline)
#
# for n, c in enumerate(candidates):
#     # print('------------')
#     for path in c['src_path']:
#         if '2902' in path['src_path']:
#             print(n, path['src_path'])


for n, p in enumerate(Queue.objects):
    if p.src_metadata:
        t_orig = p.src_metadata.creationTime
        if t_orig:
            t = "".join([x for x in t_orig if x in "0123456789:/ "])  # Strip extraneous characters
        try:
            arrow.get(t, ['YYYY:MM:DD HH:mm:ss', 'YYYY:MM:DD HH:mm', 'DD/MM/YYYY'])
        except ValueError as e:
            print(f"Value error! {n}: {t} {t.encode()} {e} {p.src_path}")
print(f"Done! {n} photos checked")


