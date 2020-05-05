import argparse
from dataclasses import dataclass
import errno
import logging
import multiprocessing
import operator
import os
import pathlib
import sys
from typing import List

import tensorflow as tf
from tqdm import tqdm
import ulid


@dataclass
class Status:
    code: int
    message: str

    @staticmethod
    def success() -> "Status":
        return Status(code=0, message="")

    @staticmethod
    def error(message: str) -> "Status":
        return Status(code=1, message=message)

    @staticmethod
    def wrap(wrapped: "Status", message: str) -> "Status":
        return Status(code=wrapped.code, message=f"{message}: {wrapped.message}")

    def ok(self) -> bool:
        return self.code == 0


class Worker:
    out_dir: str

    def __init__(self, out_dir: str) -> None:
        self.out_dir = out_dir

    def create_example(self, name: str, age: int) -> tf.train.Example:
        return tf.train.Example(
            features=tf.train.Features(
                feature={
                    "name": tf.train.Feature(bytes_list=tf.train.BytesList(value=[name.encode("utf-8")])),
                    "age": tf.train.Feature(int64_list=tf.train.Int64List(value=[age])),
                }
            )
        )

    def write_example(self, example: tf.train.Example) -> Status:
        with open(os.path.join(self.out_dir, str(ulid.new()) + ".tfexample"), "w") as fh:
            bytes_written = fh.write(str(example.SerializeToString()))
            if bytes_written == 0:
                return Status.error("Wrote 0 bytes to file")
        return Status.success()

    def work(self, line: str) -> Status:
        fields = [field.strip() for field in line.split(",")]
        if len(fields) != 2:
            return Status.error(f"Expected to find 2 fields but found {len(fields)}")

        example = self.create_example(fields[0], int(fields[1]))

        write_status = self.write_example(example)
        if not write_status.ok():
            return Status.wrap(write_status, "Error writing TF example file")

        return Status.success()


def count_lines_in_file(target: pathlib.Path) -> int:
    lines = 0
    with open(target) as fh:
        for _ in enumerate(fh):
            lines += 1
    return lines


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data_dir", type=pathlib.Path, help="directory of data files", default="./data/names")
    parser.add_argument("--out_dir", type=pathlib.Path, help="directory to output data files", default="output")
    parser.add_argument("--num_workers", type=int, help="number of worker processes", default=multiprocessing.cpu_count())
    args = parser.parse_args(sys.argv[1:])

    logging.basicConfig(
        format="level=%(levelname)s time=%(asctime)s %(message)s", datefmt="%Y-%m-%dT%H:%M:%S%z", level=logging.DEBUG,
    )

    try:
        os.makedirs(args.out_dir)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise e

    pool = multiprocessing.Pool(args.num_workers)
    total_inputs = 0

    files = sorted([args.data_dir / x for x in os.listdir(args.data_dir) if x.endswith(".csv")])

    logging.info(f"Counting the number of lines in the {len(files)} supplied files")
    for line_count in tqdm(pool.imap_unordered(count_lines_in_file, files), total=len(files)):
        total_inputs += line_count
    logging.info(f"Found {total_inputs} total inputs")

    with tqdm(total=total_inputs) as progress:
        worker = Worker(out_dir=args.out_dir)
        for f in files:
            with open(f, "r") as lines:
                for status in pool.imap(worker.work, lines):
                    if not status.ok():
                        logging.error(f"Error processing line in {f}: {status.message}")
                    progress.update()

    pool.close()
    pool.join()
    logging.info("All done!")
