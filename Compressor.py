from typing import List
import os
import sqlite3 as sql


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

    def create_table(self, file_db: str):
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
        cur = conn.cursor()
        cur.execute(CREATE_TABLE_STATEMENT)
        conn.commit()
        print(f"SQL DB created at {file_db}")

        return conn

    def put_to_db(self, byte_chunk: str, conn: sql.Connection) -> str:
        """
        Puts new byte chunk into DB. Returns either stored or newly generated hash.
        :param conn: connection to SQLite3 DB.
        :return: hash str
        """

        cur = conn.cursor()
        chunk_hash = self.HASH_FUNC(byte_chunk)

        row = cur.execute("""SELECT * FROM chunk_table WHERE hash = ?;""", (chunk_hash,)).fetchone()

        if row is None:
            cur.execute("""INSERT INTO chunk_table (hash, data, reuse_cnt) VALUES (?, ?, ?);""", (chunk_hash, byte_chunk, 1))
            conn.commit()
            row = cur.execute("""SELECT * FROM chunk_table WHERE hash = ?;""", (chunk_hash,)).fetchone()
            return row[0]
        else:
            row_id = row[0]
            reuse_cnt = row[3]
            cur.execute("""UPDATE chunk_table SET reuse_cnt = ? WHERE id = ?;""", (reuse_cnt + 1, row_id))
            conn.commit()

            return row_id;


    def compress(self, file_to_compress) -> List[str]:
        db_file = os.path.splitext(file_to_compress)[0] + ".db"
        compressed_file = os.path.splitext(file_to_compress)[0] + ".bin"

        conn = self.create_table(db_file)

        for byte_chunk in self.split_to_chunks(file_to_compress):
            id = self.put_to_db(byte_chunk, conn)
            print(f"Chunk: {byte_chunk}  -- {id}, {type(id)}")




        hash_chain = []
        chunk_data_dict = {}





if __name__ == "__main__":
    c = Compressor()
    file = "/home/jegor/PycharmProjects/sabd_data_deduplication/test_files/test.txt"
    c.compress(file)



