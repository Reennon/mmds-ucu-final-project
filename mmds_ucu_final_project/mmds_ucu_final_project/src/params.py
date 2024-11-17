from pydantic import BaseModel


class Parameters(BaseModel):
    port: int = 9001
    bloom_filter_error_rate: float = 0.1
    min_num_edits: int = 40_000
    rdd_sampling: float = 0.2


params = Parameters()
