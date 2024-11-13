from pydantic import BaseModel


class Parameters(BaseModel):
    port: int = 9000
    bloom_filter_error_rate: float = 0.1
    min_num_edits: int = 400
    rdd_sampling: float = 0.2


params = Parameters()
