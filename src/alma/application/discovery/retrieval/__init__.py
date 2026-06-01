"""Discovery retrieval layer.

The four candidate-retrieval channels (lexical / vector / graph / external)
plus the channel-merge & diversity-selection helpers, split out of the
discovery god-module (D-9). Every name is re-exported from
``alma.application.discovery`` for backward compatibility.
"""

from ._common import (
    _GRAPH_FALLBACK_DEADLINE_S,
    _candidate_author_keys,
    _candidate_key,
    _candidate_source_bucket,
    _candidate_topic_keys,
    _candidate_venue_key,
    _drain_futures_within_deadline,
)
from .merge import (
    _merge_channel_candidates,
    _recommendation_mix_summary,
    _select_diverse_recommendation_candidates,
)
from .lexical import _retrieve_lexical_channel
from .vector import _retrieve_vector_channel
from .graph import _retrieve_graph_channel
from .external import _retrieve_external_channel

__all__ = [
    "_GRAPH_FALLBACK_DEADLINE_S",
    "_candidate_author_keys",
    "_candidate_key",
    "_candidate_source_bucket",
    "_candidate_topic_keys",
    "_candidate_venue_key",
    "_drain_futures_within_deadline",
    "_merge_channel_candidates",
    "_recommendation_mix_summary",
    "_select_diverse_recommendation_candidates",
    "_retrieve_lexical_channel",
    "_retrieve_vector_channel",
    "_retrieve_graph_channel",
    "_retrieve_external_channel",
]
