import json
import time

from unsync import unsync, Unfuture
from pathlib import Path
from photo_file_metadata import photo_file_metadata
from dataclasses import asdict
from collections import deque
from itertools import islice, filterfalse

from loguru import logger
import datetime

logger.add("update_metadata.log", rotation="100 MB")  # TODO:  Redirect logging to stderr

root = r"D:\Takeout\Takeout\Google Photos"
sink = Path(r'C:\Users\Scott\Documents\Programming\PhotoManager\20210706_takeout_metadata.json')
batchsize = 100
queue_depth = 100


def main():
    unfuture_queue = deque()
    completed = completed_set(sink)
    all_filesystem_paths = (p for p in Path(root).rglob('*') if p.is_file())
    unprocessed_paths = filterfalse(lambda p: str(p) in completed, all_filesystem_paths)
    while True:
        for p in islice(unprocessed_paths, queue_depth - (len(unfuture_queue) or 0)):
            assert str(p) not in completed, "Error! File has already been processed"
            unfuture_queue.append(update_metadata(p))
        if not len(unfuture_queue):
            break
        with sink.open(mode='a+') as fp:
            for f in list(unfuture_queue):
                if f.done():
                    fp.write(f"{f.result()}\n")
                    unfuture_queue.remove(f)
    logger.info("Done!")


@unsync(cpu_bound=True)
def update_metadata(path):
    return json.dumps(asdict(photo_file_metadata(path)))


def completed_set(path):
    logger.info(f"Scanning for completed files...")
    completed = set()
    if not path.is_file():
        return completed
    with path.open() as fp:
        while True:
            line = fp.readline()
            if not line:
                break
            try:
                completed.add(json.loads(line).get('path'))
            except Exception as e:
                logger.info(f"{line}: {e}")
    logger.info(f"Number of files already scanned: {len(completed)}")
    return completed


if __name__ == "__main__":
    main()
