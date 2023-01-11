import sqlite3 as sql
from typing import Tuple
import os


class Duplicator:
    def __init__(self):
        ...

    def duplicate(self, file_bin: str, file_db: str) -> str:
        """
        Duplicates (restores, decompresses) the files back

        :param file_db:
        :param file_bin:
        :return: path to deduplicated file
        """

        file_stem = os.path.splitext(file_bin)[0]

        if not os.path.exists(file_bin):
            raise ValueError(f"Expected BIN file {file_bin} does not exists.")
        if not os.path.exists(file_db):
            raise ValueError(f"Expected DB file {file_db} does not exists.")

        conn = sql.connect(file_db)

        (byte_size, file_extension) = self.get_metainfo(file_bin)

        file_duplicated = file_stem + ".restored" + file_extension

        with open(file_duplicated, "wb") as f:
            for bin_id in self.read_byte_file(file_bin, byte_size=byte_size, chunk_read=1):
                int_id = int.from_bytes(bin_id, byteorder="big", signed=False)
                chunk = self.get_bytes_from_db(int_id, conn)
                f.write(chunk)

        return file_duplicated

    @staticmethod
    def get_metainfo(file_bin: str) -> Tuple[int, str]:
        """
        Reads metainfo from BIN file
        :param file_bin: path ti BIN file
        :return: byte_size and extension of deduplicated file
        """
        with open(file_bin, "rb") as f:
            chunk = f.read(8)
        byte_size = chunk[0]
        extension = chunk[1:].decode("utf-8").strip()
        return byte_size, extension

    @staticmethod
    def get_bytes_from_db(chunk_id: int, conn: sql.Connection) -> str:
        data_row = conn.execute("""SELECT data FROM main.chunk_table WHERE id = ?""", (chunk_id,)).fetchone()
        if data_row is not None:
            return data_row[0]
        else:
            raise ValueError(f"ID {chunk_id} not found.")

    @staticmethod
    def read_byte_file(file_bin, byte_size, chunk_read=0):
        with open(file_bin, "rb") as f:

            # Skipping metainfo
            f.read(8)

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
                        yield chunk[i:i + byte_size]



if __name__ == '__main__':
    d = Duplicator()
    file = "/home/jegor/PycharmProjects/sabd_data_deduplication/test_files/photo.bin"
    for b in d.read_byte_file(file, 4, chunk_read=1):
        print(b)


