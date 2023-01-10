from typing import List, Tuple
import os
import sqlite3 as sql
from math import ceil


class Compressor:
    def __init__(self, hash_funk=lambda x: x, chunk_size=10):
        """
        Compresses files.

        :param hash_funk: function to compute hash. Default: None.
        :param chunk_size: size of a byte chunk to split the file into.
        """

        self.HASH_FUNC = hash_funk
        self.CHUNK_SIZE = chunk_size

    def split_to_chunks(self, file_to_split):
        """
        Generator. Splits file into byte chunks.

        :param file_to_split:
        :return:
        """

        with open(file_to_split, "rb") as f:
            print("File opened")
            while True:
                chunk = f.read(self.CHUNK_SIZE)

                if len(chunk) == 0:
                    break
                if len(chunk) < self.CHUNK_SIZE:
                    chunk = chunk.ljust(self.CHUNK_SIZE, b"\0")
                # print(chunk)

                yield chunk

    def create_database(self, file_db: str):
        CREATE_TABLE_STATEMENT = """
            CREATE TABLE IF NOT EXISTS chunk_table(
                id INTEGER PRIMARY KEY,
                hash TEXT,
                data TEXT,
                reuse_cnt INTEGER,
                rational_id INTEGER
            );
        """

        conn = sql.connect(file_db)
        conn.execute(CREATE_TABLE_STATEMENT)
        conn.execute("""PRAGMA synchronous = OFF;""")
        # conn.execute("""PRAGMA journal_mode = WAL;""")
        conn.commit()
        print(f"SQL DB created at {file_db}")

        return conn

    def save_byte_size(self, byte_size: int, conn: sql.Connection):
        conn.execute("""CREATE TABLE IF NOT EXISTS metainfo(byte_size INTEGER);""")
        conn.commit()
        conn.execute("""INSERT INTO metainfo (byte_size) VALUES (?)""", (byte_size,))
        conn.commit()

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

    def compress(self, file_to_compress) -> Tuple[str, str]:
        db_file = os.path.splitext(file_to_compress)[0] + ".db"
        compressed_file = os.path.splitext(file_to_compress)[0] + ".bin"

        conn = self.create_database(db_file)
        id_arr = []

        for byte_chunk in self.split_to_chunks(file_to_compress):
            id = self.put_to_db(byte_chunk, conn)
            id_arr.append(id)

        max_byte_len = ceil((len(bin(max(id_arr))) - 2) / 8)
        self.save_byte_size(max_byte_len, conn)

        with open(compressed_file, "wb") as f:
            f.write(b"".join(i.to_bytes(length=max_byte_len, byteorder="big", signed=False) for i in id_arr))

        return compressed_file, db_file


if __name__ == "__main__":
    c = Compressor()
    file = "/home/jegor/PycharmProjects/sabd_data_deduplication/test_files/photo.jpg"
    c.compress(file)



