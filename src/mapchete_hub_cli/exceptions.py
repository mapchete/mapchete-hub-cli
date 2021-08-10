class UnknownJobState(Exception):
    """Raise if job is registered but has unknown state."""


class JobNotStarted(Exception):
    """Raise if job is registered but has not started yet."""


class JobFailed(Exception):
    """Raise if job is registered but has failed."""


class JobNotFound(Exception):
    """Raise if job could not be found."""


class JobRejected(Exception):
    """Raise if job is rejected from server."""
