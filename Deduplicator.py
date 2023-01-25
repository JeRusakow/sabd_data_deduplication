from typing import List, Tuple
import os
import sqlite3 as sql
from math import ceil


class Deduplicator:
    def __init__(self, hash_funk=lambda x: x, chunk_size=10, chunk_read=0):
        """
        Compresses files.

        :param hash_funk: function to compute hash. Default: None.
        :param chunk_size: size of a byte chunk to split the file into.
        """

        self.HASH_FUNC = hash_funk
        self.CHUNK_SIZE = chunk_size
        self.CHUNK_READ = chunk_read

    def split_to_chunks(self, file_to_split):
        """
        Generator. Splits file into byte chunks.

        :param file_to_split:
        :return:
        """

        with open(file_to_split, "rb") as f:
            if self.CHUNK_READ > 0:
                while True:
                    chunk = f.read(self.CHUNK_SIZE * self.CHUNK_READ)

                    if len(chunk) == 0:
                        break

                    for i in range(0, self.CHUNK_SIZE * self.CHUNK_READ, self.CHUNK_SIZE):
                        yield chunk[i: i + self.CHUNK_SIZE]
            else:
                chunk = f.read()
                for i in range(0, len(chunk), self.CHUNK_SIZE):
                    yield chunk[i: i + self.CHUNK_SIZE]

    @staticmethod
    def create_database(file_db: str):
        CREATE_DATA_TABLE_STATEMENT = """
            CREATE TABLE IF NOT EXISTS chunk_table(
                id INTEGER PRIMARY KEY,
                hash TEXT,
                data TEXT,
                reuse_cnt INTEGER
            );
        """

        CREATE_HASH_COLUMN_INDEX = """
            CREATE UNIQUE INDEX IF NOT EXISTS hash_index
            ON chunk_table (hash);"""

        conn = sql.connect(file_db)
        conn.execute(CREATE_DATA_TABLE_STATEMENT)
        conn.execute(CREATE_HASH_COLUMN_INDEX)
        conn.execute("""PRAGMA synchronous = OFF;""")
        # conn.execute("""PRAGMA journal_mode = WAL;""")
        conn.commit()

        return conn

    def put_to_db(self, byte_chunk: str, conn: sql.Connection) -> str:
        """
        Puts new byte chunk into DB. Returns either stored or newly generated hash.
        :param conn: connection to SQLite3 DB.
        :return: hash str
        """

        chunk_hash = self.HASH_FUNC(byte_chunk)

        row = conn.execute("""SELECT * FROM chunk_table WHERE hash = ?;""", (chunk_hash,)).fetchone()

        if row is None:
            conn.execute("""INSERT INTO chunk_table (hash, data, reuse_cnt) VALUES (?, ?, ?);""", (chunk_hash, byte_chunk, 1))
            conn.commit()
            row = conn.execute("""SELECT * FROM chunk_table WHERE hash = ?;""", (chunk_hash,)).fetchone()
            return row[0]
        else:
            row_id = row[0]
            reuse_cnt = row[3]
            conn.execute("""UPDATE chunk_table SET reuse_cnt = ? WHERE id = ?;""", (reuse_cnt + 1, row_id))
            conn.commit()

            return row_id;

    def deduplicate(self, file_to_deduplicate: str, db_file: str) -> str:
        """
        Deduplicates the file.

        :param db_file: path to database
        :param file_to_deduplicate: path to file to deduplicate
        :return: path to BIN file
        """
        file_bin = os.path.splitext(file_to_deduplicate)[0] + ".bin"
        old_extension = os.path.splitext(file_to_deduplicate)[1]

        conn = self.create_database(db_file)
        id_arr = []

        for byte_chunk in self.split_to_chunks(file_to_deduplicate):
            id = self.put_to_db(byte_chunk, conn)
            id_arr.append(id)

        max_byte_len = ceil((len(bin(max(id_arr))) - 2) / 8.)

        # this will never happen :)
        if max_byte_len >= 255:
            raise ValueError(f"Hash table overfilled")

        # metainfo written to BIN file
        metainfo = max_byte_len.to_bytes(length=1, byteorder="big", signed=False) + old_extension.ljust(7).encode("utf-8")

        conn.close()

        with open(file_bin, "wb") as f:
            f.write(metainfo)
            f.write(b"".join(i.to_bytes(length=max_byte_len, byteorder="big", signed=False) for i in id_arr))

        return file_bin


if __name__ == "__main__":
    c = Deduplicator()
    file = "/home/jegor/PycharmProjects/sabd_data_deduplication/test_files/photo.jpg"




