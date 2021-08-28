import datetime
from pathlib import Path
from typing import List

from loguru import logger
import arrow

from me_models import DbConnect, Queue
from utils import config

WRITE_BATCH_SIZE = 1000

cfg = config()
logger.add("build_queue.log", rotation="1 MB")
DbConnect()


def update_candidates(dirlist: List[str]):
    logger.info(f"Walking target list: {dirlist}")
    total_files = 0
    batch = []
    excluded = set()
    for top in dirlist:
        logger.info(f"Traversing tree at {top} and adding to queue.")
        pathset = set()
        for n, f in enumerate(Queue.objects()):
            pathset.add(f.src_path)
            if n % 10000 == 0:
                logger.info(f"Processed {n}")
        for n, path in enumerate(Path(top).rglob("**/*")):
            if n % 10000 == 0:
                logger.info(f"Procesed: {n}")
            if not path.is_file():
                continue
            ext = path.suffix.lower()
            if ext not in cfg.settings.image_filetypes:
                excluded.update([ext.replace(".", "")])
                continue
            if str(path) not in pathset:
                logger.info(f"Adding: {path}")
                batch.append(
                    Queue(
                        src_path=str(path),
                        src_filename=str(path.name),
                        size=path.stat().st_size,
                        modifiedTime=arrow.get(path.stat().st_mtime).datetime
                    )
                )
                if len(batch) >= WRITE_BATCH_SIZE:
                    total_files = write_batch(batch, total_files)
                    batch = []
    write_batch(batch, total_files)
    return excluded


def write_batch(batch, total_files):
    if len(batch):
        Queue.objects.insert(batch)
        total_files += len(batch)
        logger.info(f"Saved {total_files}")
    return total_files


if __name__ == "__main__":
    dirlist = [r"D:\Gphotos Mirror\Google Photos"]
    logger.info("Starting queue update")
    start = datetime.datetime.now()
    excluded = update_candidates(dirlist)
    elapsed = datetime.datetime.now() - start
    logger.info(
        f"Done queue update. Elapsed time: {elapsed}. Excluded file types: {excluded}"
    )
