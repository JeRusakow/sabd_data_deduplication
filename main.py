import time

from sabd_sys import Sabd
import pandas as pd
from argparse import ArgumentParser


BYTE_CHUNK_SIZES = [10, 20, 32, 64, 96, 128, 160, 192, 256, 384, 512]
HASH_FUNCTIONS = ["none", "md5", "sha1", "sha256", "sha512"]


def dict_to_str(d: dict) -> str:
    return "\n".join([f"{k}: {v}" for k, v in d.items()])


def ask_yes_no_question(question: str) -> bool:
    print(question + " (yes / no)")
    ans = input()
    if ans in ["y", "yes"]:
        return True
    elif ans in ["n", "no"]:
        return False
    else:
        return ask_yes_no_question(question)




if __name__ == '__main__':

    parser = ArgumentParser(
        prog="Deduplication system"
    )

    parser.add_argument(
        "-db",
        "--db-path",
        help="Path to existing DB or where new one should be placed",
        type=str
    )
    parser.add_argument(
        "-in",
        "--input-files",
        help="File or directory with input files",
        type=str
    )
    parser.add_argument(
        "-m",
        "--mode",
        help="Run mode",
        choices=["Test", "Deduplication", "Duplication"]
    )
    parser.add_argument(
        "-hf",
        "--hash-function",
        help="Hash function to use",
        choices=["none", "md5", "sha1", "sha256", "sha512"],
    )
    parser.add_argument(
        "-b",
        "--byte-chunk-size",
        help="Size of chunk in bytes for deduplication",
        type=int
    )
    parser.add_argument(
        "-l",
        "--log-path",
        help="Path to store log at. Only for 'Test' mode",
        type=str
    )
    parser.add_argument(
        "-i",
        "--iterations",
        help="Number of iteration for Test mode",
        type=int,
        default=2
    )

    args = parser.parse_args()

    # print(args)

    if args.mode == "Test":

        statis_table = None

        for chunk_size in BYTE_CHUNK_SIZES:
            for hash_func in HASH_FUNCTIONS:
                s = Sabd(
                    target_path=args.input_files,
                    db_path=args.db_path,
                    byte_chunk_size=chunk_size,
                    hash_func=hash_func,
                    chunk_rw=1
                )

                stats = s.run_several_times(args.iterations)
                if statis_table is None:
                    statis_table = pd.Series(stats).to_frame()
                else:
                    statis_table = pd.concat((statis_table, pd.Series(stats).to_frame()), axis=1)

                statis_table.transpose().to_csv(args.log_path, sep=",")

    elif args.mode == "Deduplication":

        s = Sabd(
            target_path=args.input_files,
            db_path=args.db_path,
            byte_chunk_size=args.byte_chunk_size,
            hash_func=args.hash_function,
            chunk_rw=1
        )

        if ask_yes_no_question("Continue deduplication?") is False:
            print("Aborting")
            quit()

        t1 = time.time()
        s.deduplicate_all(s.target_files)
        t2 = time.time()

        print(f"Deduplication done in {t2 - t1:.2f} seconds.")

    elif args.mode == "Duplication":
        s = Sabd(
            target_path=args.input_files,
            db_path=args.db_path,
            byte_chunk_size=10,
            hash_func="none",
            chunk_rw=1
        )

        if ask_yes_no_question("Only BIN files can be duplicated. Other ones will cause failure. Continue duplication?") is False:
            print("Aborting")
            quit()

        t1 = time.time()
        s.duplicate_all(s.target_files)
        t2 = time.time()

        print(f"Duplication done in {t2 - t1:.2f} seconds.")

    else:
        print("Mode unstated")





