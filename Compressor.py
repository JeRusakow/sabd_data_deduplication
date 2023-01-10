from typing import List


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



    def compress(self, file_to_compress) -> List[str]:
        ...


if __name__ == "__main__":
    c = Compressor()
    byte_arr = [i for i in c.split_to_chunks("/home/jegor/PycharmProjects/sabd_data_deduplication/test_files/photo.txt")]



