import datetime
from collections import Counter
from pathlib import Path
from typing import List

from loguru import logger
import arrow

from me_models import DbConnect, Queue
from utils import config

cfg = config()
logger.add("build_queue.log", rotation="1 MB")
DbConnect()


def add_candidates(dirlist: List[str]):
    logger.info(f"Walking target list: {dirlist}")
    excluded = Counter()
    for top in dirlist:
        logger.info(f"Traversing tree at {top} and adding to queue.")
        top_path = Path(top)
        for path in top_path.rglob("**/*"):
            ext = path.suffix.lower()
            if ext in cfg.settings.image_filetypes:
                Queue(
                    src_path=str(path),
                    src_filename=str(path.name),
                    size=path.stat().st_size,
                    modifiedTime=arrow.get(path.stat().st_mtime).datetime
                ).save()
            else:
                excluded.update([ext.replace(".", "")])
                logger.info(f"Skipped file: {path}")
    return excluded


if __name__ == "__main__":
    dirlist = [r"D:\Gphotos Mirror\Google Photos"]
    while True:
        response = input("Clear Queue? (y/n)")
        if response == "y":
            Queue.drop_collection()
            logger.info("********** Starting new run with cleared queue **********")
            break
        if response == "n":
            logger.info("********** Starting new run with existing queue **********")
            break
    logger.info("Starting queue update")
    start = datetime.datetime.now()
    excluded = add_candidates(dirlist)
    elapsed = datetime.datetime.now() - start
    logger.info(
        f"Done queue update. Elapsed time: {elapsed}. Excluded file types: {excluded}"
    )
