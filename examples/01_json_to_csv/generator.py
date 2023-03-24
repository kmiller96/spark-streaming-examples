"""Script to continuously create data.

Generates a faux dataset composed of many JSONLines files which are "streamed"
in.
"""

# pylint: disable=invalid-name,global-statement,redefined-builtin


import json
import time
import random
import shutil
from pathlib import Path

import fire


###############
## Utilities ##
###############


def generate_record(id: int):
    """Creates a new record."""
    return {"id": id, "n": round(random.random(), 4)}


def generate_file(fpath: str, *, start: int = 0, end: int = 1000):
    """Creates a new file.

    Args:
        fpath (str): Path to the file to create.
        start (int): Starting index.
        end (int): Ending index.
    """
    content = ""

    for i in range(start, end):
        content += json.dumps(generate_record(i)) + "\n"

    with open(fpath, "w", encoding="utf-8") as f:
        f.write(content)

    return fpath


##################
## Main Routine ##
##################


def main(unload_dir: str, n_files: int = -1, fps: float = 0.5, rpf: int = 1000):
    """Generates data according to the CLI configuration.

    Args:
        unload_dir (str): Directory to write files to.
        n_files (int): Number of files to generate. If -1, generate forever.
        fps (float): Number of files to generate per second.
        rpf (int): Number of rows to generate per file.
    """
    d = Path(unload_dir)

    shutil.rmtree(d, ignore_errors=True)
    d.mkdir(parents=True, exist_ok=True)

    i = 0

    while n_files != 0:
        start, end = i, i + rpf
        name = f"{str(i // rpf).zfill(5)}.json"

        print(f" File: {name} (id: {start} -> {end})", end="\r")
        generate_file(d / name, start=start, end=end)

        n_files -= 1
        i = end

        time.sleep(1 / fps)


if __name__ == "__main__":
    fire.Fire(main)
