"""Distributed sparse Structure-from-Motion orchestration (COLMAP/pycolmap)."""

import time
from pathlib import Path

from pyspark.sql import functions as F


def image_ids_to_pair_id(image_id1, image_id2):
    if image_id1 > image_id2:
        image_id1, image_id2 = image_id2, image_id1
    return 2147483647 * image_id1 + image_id2
