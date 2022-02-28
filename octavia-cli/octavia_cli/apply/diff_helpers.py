#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import hashlib

from deepdiff import DeepDiff

SECRET_MASK = "**********"


def compute_checksum(configuration_path):
    BLOCK_SIZE = 65536
    file_hash = hashlib.sha256()
    with open(configuration_path, "rb") as f:
        fb = f.read(BLOCK_SIZE)
        while len(fb) > 0:
            file_hash.update(fb)
            fb = f.read(BLOCK_SIZE)
    return file_hash.hexdigest()


def exclude_secrets_from_diff(obj, path):
    if isinstance(obj, str):
        return True if SECRET_MASK in obj else False
    else:
        return False


def compute_diff(a, b):
    return DeepDiff(a, b, view="tree", exclude_obj_callback=exclude_secrets_from_diff)
