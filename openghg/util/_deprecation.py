"""Helpers for deprecated compatibility arguments.

Use :func:`_warn_if_force_ignored` at public entry points that retain the
ignored ``force`` parameter. The helper emits a caller-attributed
``DeprecationWarning`` only for true values and does not change overlap
handling.
"""

import warnings


def _warn_if_force_ignored(force: bool) -> None:
    """Warn the API caller when the deprecated ``force`` argument is ignored.

    Args:
        force: Whether the deprecated compatibility argument was enabled.

    Warns:
        DeprecationWarning: If ``force`` is true.
    """
    if force:
        warnings.warn(
            "The force argument is deprecated and ignored; use if_exists to control "
            "how existing data is handled.",
            DeprecationWarning,
            stacklevel=3,
        )
