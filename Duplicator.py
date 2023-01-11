import sqlite3 as sql
from typing import Tuple
import os


class Duplicator:
    def __init__(self):
        ...

    def duplicate(self, file: str):
        """
        Duplicates (restores, decompresses) the files back
        :param file: Path to either .DB or .BIN files of deduplicated object
        :return: path to deduplicated file
        """

        file_stem = os.path.splitext(file)[0]
        file_bin = file_stem + ".bin"
        file_db = file_stem + ".db"

        if not os.path.exists(file_bin):
            raise ValueError(f"Expected BIN file {file_bin} does not exists.")
        if not os.path.exists(file_db):
            raise ValueError(f"Expected DB file {file_db} does not exists.")

        conn = sql.connect(file_db)

        (byte_size, file_extension) = self.get_metainfo(conn)

        file_duplicated = file_stem + ".restored" + file_extension

        with open(file_duplicated, "wb") as f:
            for bin_id in self.read_byte_file(file_bin, byte_size=byte_size, chunk_read=1):
                int_id = int.from_bytes(bin_id, byteorder="big", signed=False)
                chunk = self.get_bytes_from_db(int_id, conn)
                f.write(chunk)

    def get_metainfo(self, conn: sql.Connection) -> Tuple[int, str]:
        metainfo = conn.execute("""SELECT byte_size, file_extension FROM metainfo;""").fetchone()
        if metainfo is not None:
            return metainfo
        else:
            raise ValueError("Error fetching metainfo from DB")

    def get_bytes_from_db(self, chunk_id: int, conn: sql.Connection) -> str:
        data_row = conn.execute("""SELECT data FROM main.chunk_table WHERE id = ?""", (chunk_id,)).fetchone()
        if data_row is not None:
            return data_row[0]
        else:
            raise ValueError(f"ID {chunk_id} not found.")

    def read_byte_file(self, file_bin, byte_size, chunk_read=0):
        with open(file_bin, "rb") as f:
            if chunk_read <= 0:
                byte_arr = f.read()
                for i in range(0, len(byte_arr), byte_size):
                    yield byte_arr[i:i + byte_size]
            else:
                while True:
                    chunk = f.read(byte_size * chunk_read)

                    if len(chunk) == 0:
                        break

                    for i in range(0, len(chunk), byte_size):
                        a = chunk[i:i + byte_size]
                        yield a



if __name__ == '__main__':
    d = Duplicator()
    file = "/home/jegor/PycharmProjects/sabd_data_deduplication/test_files/photo.bin"
    for b in d.read_byte_file(file, 4, chunk_read=1):
        print(b)


