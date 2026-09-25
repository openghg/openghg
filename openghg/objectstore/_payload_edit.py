"""Lifecycle of one unpublished payload, independent of catalog persistence."""

from collections.abc import Callable

from openghg.storage._zarr_store import ZarrStore
from openghg.types import ObjectStoreError


class PayloadEdit:
    """Hold a working store and backend callbacks for sealing and cleanup.

    The reference stays opaque to the editor. Once publication has been attempted,
    abort retains the payload because the publication may have succeeded remotely.
    """

    def __init__(
        self,
        store: ZarrStore,
        reference: str,
        *,
        check_current: Callable[[], None],
        abort: Callable[[], None],
        finish: Callable[[], str] | None = None,
    ) -> None:
        self.store = store
        self.reference = reference
        self._check = check_current
        self._abort = abort
        self._finish = finish
        self._finished = False
        self._aborted = False
        self._publishing = False

    def check_current(self) -> None:
        """Check writer ownership and the publication revision captured at begin."""
        if self._aborted:
            raise ObjectStoreError("This payload edit was aborted; start a new edit.")
        self._check()

    def finish(self) -> str:
        """Seal the payload and return its reference without publishing a version."""
        self.check_current()
        if not self._finished:
            if self._finish is not None:
                self.reference = self._finish()
            self._finished = True
        return self.reference

    def mark_publishing(self) -> None:
        """Retain the payload from this point, including on ambiguous write errors."""
        if not self._finished:
            raise ObjectStoreError("Finish the payload before publishing it.")
        self._publishing = True

    def abort(self) -> None:
        """Invalidate the edit and remove only definitely unpublished payloads."""
        if self._aborted:
            return
        self._aborted = True
        if not self._publishing:
            self._abort()
