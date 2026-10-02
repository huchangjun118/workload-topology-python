"""Error types for the topology service.

NotFound and Forbidden are deliberately distinct so that callers can
tell "entity does not exist" apart from "entity exists but is out of
the caller's scope".
"""

from __future__ import annotations


class TopologyError(Exception):
    """Base class for all topology service errors."""


class NotFoundError(TopologyError):
    """The requested entity does not exist in the aggregated view."""


class ForbiddenError(TopologyError):
    """The requested entity exists but is outside the caller's scope."""


class CancelledError(TopologyError):
    """The read request was cancelled by the caller."""


class TimeoutError(TopologyError):
    """The read request exceeded its deadline."""
