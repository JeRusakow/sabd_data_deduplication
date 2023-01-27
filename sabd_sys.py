from typing import List

from Duplicator import Duplicator
from Deduplicator import Deduplicator
from hashlib import md5, sha512, sha256, sha1
import os
import time
from itertools import zip_longest
from statistics import mean


class Sabd:
    def __init__(self, target_path: str, db_path: str, byte_chunk_size: int, hash_func: str, chunk_rw=1):
        """

        :param target_path:
        :param db_path:
        :param byte_chunk_size:
        :param hash_func:
        :param chunk_rw:
        """

        self.hash_funk_name = hash_func

        if hash_func == "md5":
            self.hash_funk = lambda x: md5(x).digest()
        elif hash_func == "sha512":
            self.hash_funk = lambda x: sha512(x).digest()
        elif hash_func == "sha256":
            self.hash_funk = lambda x: sha256(x).digest()
        elif hash_func == "sha1":
            self.hash_funk = lambda x: sha1(x).digest()
        elif hash_func == "none":
            self.hash_funk = lambda x: x
        else:
            raise ValueError(f"Hash function '{hash_func} not specified")

        self.deduplicator = Deduplicator(hash_funk=self.hash_funk, chunk_size=byte_chunk_size, chunk_read=chunk_rw)
        self.duplicator = Duplicator()
        self.byte_chunk_size = byte_chunk_size

        if os.path.isdir(target_path):
            self.target_files = [os.path.join(target_path, file) for file in os.listdir(target_path)]
        else:
            self.target_files = [target_path]

        print(f"Detected {len(self.target_files)} files:")
        for file in self.target_files:
            print(f"{file}")

        self.file_db = db_path

        self.stats = {
            "dedulication_time": [],
            "duplication_time": []
        }

    def deduplicate_all(self, files_target: List[str]) -> List[str]:
        res_files = []

        for file in files_target:
            print(f"Deduplicating {file} ...")
            res = self.deduplicator.deduplicate(file, self.file_db)
            res_files.append(res)

        return res_files

    def duplicate_all(self, files_bin: List[str]) -> List[str]:
        res_files = []

        for file in files_bin:
            print(f"Duplicating {file} ...")
            res = self.duplicator.duplicate(file, self.file_db)
            res_files.append(res)

        return res_files

    @staticmethod
    def error_check(files_in: List[str], files_out: List[str]) -> dict:
        res = {
            "file": [],
            "length": [],
            "error_count": [],
            "length_eq": []
        }

        for file_a, file_b in zip(files_in, files_out):
            data_a = open(file_a, "rb").read()
            data_b = open(file_b, "rb").read()
            err_count = 0

            for x, y in zip_longest(data_a, data_b):
                if x != y:
                    err_count += 1

            if len(data_a) != len(data_b):
                print(f"Files {file_a} and {file_b} differ in length: {len(data_a)} VS {len(data_b)}.")
                res["length_eq"].append(True)
            else:
                res["length_eq"].append(False)

            res["file"].append(file_a)
            res["length"].append(max([len(data_a), len(data_b)]))
            res["error_count"].append(err_count)

        return res

    def run_once(self):
        res = {
            "byte_chunk_size": self.byte_chunk_size
        }

        print("\nDeduplication started")
        time_start = time.time()
        dedup_files = self.deduplicate_all(self.target_files)
        time_end = time.time()

        res["deduplication_time"] = time_end - time_start
        print(f"Done in {res['deduplication_time']}")

        print("\nDuplication started")
        time_start = time.time()
        dup_files = self.duplicate_all(dedup_files)
        time_end = time.time()

        res["duplication_time"] = time_end - time_start
        print(f"Done in {res['duplication_time']}")

        err_report = self.error_check(self.target_files, dup_files)
        res["length_total"] = sum(err_report["length"])
        res["error_total"] = sum(err_report["error_count"])
        res["length_violation"] = [file for (file, flag) in zip(err_report["file"], err_report["length_eq"]) if flag]

        # cleaning the environment
        for file in [*dedup_files, *dup_files, self.file_db]:
            os.remove(file)
            # ...

        return res

    def run_several_times(self, iter_cnt: int) -> dict:

        run_results = [self.run_once() for i in range(iter_cnt)]

        mean_results = {
            "iterations": iter_cnt,
            "hash_function": self.hash_funk_name,
            "byte_chunk_size": run_results[0]["byte_chunk_size"],
            "deduplication_time": mean([res["deduplication_time"] for res in run_results]),
            "duplication_time": mean([res["duplication_time"] for res in run_results]),
            "length_total": run_results[0]["length_total"],
            "error_total": run_results[0]["error_total"],
            "length_violation": run_results[0]["length_violation"]
        }

        return mean_results


if __name__ == '__main__':
    s = Sabd(
        target_path="/home/jegor/PycharmProjects/sabd_data_deduplication/sandbox",
        db_path="/home/jegor/PycharmProjects/sabd_data_deduplication/sandbox/base.db",
        byte_chunk_size=256,
        hash_func="md5",
        chunk_rw=1
    )

    data = s.run_several_times(2)
    print(data)
