from dataclasses import dataclass


@dataclass
class ChunkData():
    """
    A class to represent byte chunk info.
    """
    id: int
    hash: str
    data: str
    reuses_cnt: int
    rational_id: int
