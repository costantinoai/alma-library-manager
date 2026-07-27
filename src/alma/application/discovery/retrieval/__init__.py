"""Discovery retrieval layer.

The four candidate-retrieval channels (lexical / vector / graph / external)
plus their canonical evidence merge.
"""

from ._common import (
    _candidate_author_keys,
    _candidate_key,
    _candidate_source_bucket,
    _candidate_topic_keys,
    _candidate_venue_key,
)
from .external import _retrieve_external_channel
from .graph import _retrieve_graph_channel
from .lexical import _retrieve_lexical_channel
from .merge import (
    _merge_channel_candidates,
    _recommendation_mix_summary,
)
from .vector import _retrieve_vector_channel

__all__ = [
    "_candidate_author_keys",
    "_candidate_key",
    "_candidate_source_bucket",
    "_candidate_topic_keys",
    "_candidate_venue_key",
    "_merge_channel_candidates",
    "_recommendation_mix_summary",
    "_retrieve_lexical_channel",
    "_retrieve_vector_channel",
    "_retrieve_graph_channel",
    "_retrieve_external_channel",
]
