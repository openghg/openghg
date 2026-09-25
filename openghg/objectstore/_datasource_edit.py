"""Explicit edits to a datasource's unpublished working dataset."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    import xarray as xr

    from ._legacy_datasource import Datasource


class DatasourceEdit:
    """Collect data and attribute changes, then publish one immutable version.

    Obtain an editor with :meth:`Datasource.begin_edit`. Exiting its context
    without calling :meth:`commit` discards the working data. An operation that
    fails also discards the editor; start a new edit to retry. Changes affect
    neither saved versions nor search results until the commit succeeds.
    """

    def __init__(self, datasource: Datasource, base: str | None = "latest") -> None:
        if datasource._active_edit is not None:
            raise RuntimeError("This datasource already has an active edit.")
        if base == "latest":
            base = datasource.latest_version or None
        if base is not None and base not in datasource._data_keys:
            raise ValueError(f"Unknown base version: {base}")
        self._datasource = datasource
        self.base = base
        self._payload = datasource._begin_payload_edit(base)
        self._changed = False
        self._closed = False
        self._previewed = False
        datasource._active_edit = self

    def __enter__(self) -> DatasourceEdit:
        self._check_open()
        return self

    def __exit__(self, *exc: Any) -> None:
        self.abort()

    def _check_open(self) -> None:
        if self._closed:
            raise RuntimeError("This edit is closed. Start a new edit to make changes.")
        self._payload.check_current()

    def _close(self) -> None:
        self._closed = True
        if self._datasource._active_edit is self:
            self._datasource._active_edit = None

    def abort(self) -> None:
        """Discard unpublished changes; repeated calls have no effect.

        Working previews may become unreadable after abort. Committed readers
        are unaffected. Payloads whose publication outcome is uncertain are
        retained for recovery rather than deleted.
        """
        if self._closed:
            return
        try:
            self._payload.abort()
        finally:
            self._close()

    def _mutate(self, action: Callable[[], None], *, replaces_payload: bool = False) -> None:
        previous = None
        try:
            self._check_open()
            if self._previewed and not replaces_payload:
                from openghg.storage._zarr_copy import copy_zarr_store

                # Input can depend lazily on a working preview. Keep its source
                # unchanged until all writes complete, including disjoint regions.
                replacement = self._datasource._begin_payload_edit(None)
                try:
                    copy_zarr_store(self._payload.store.store, replacement.store.store)
                except BaseException:
                    replacement.abort()
                    raise
                previous = self._payload
                self._payload = replacement
            action()
            if previous is not None:
                previous.abort()
            self._changed = True
            self._previewed = False
        except BaseException:
            self.abort()
            raise
        finally:
            if previous is not None:
                previous.abort()

    def _validate(self, data: xr.Dataset) -> None:
        dim = self._payload.store.append_dim
        if dim not in data.coords or data.sizes.get(dim, 0) == 0:
            raise ValueError(f"Data must have a nonempty {dim!r} coordinate.")
        if not data.get_index(dim).is_unique:
            raise ValueError(f"Data must have unique {dim!r} coordinate values.")

    def _write(self, operation: str, data: xr.Dataset) -> None:
        def action() -> None:
            self._validate(data)
            store = self._payload.store
            # Appends replace global attributes in Xarray, whereas region writes
            # ignore them. Apply one explicit merge policy to both operations.
            attrs = deepcopy(data.attrs)
            variable_attrs = {name: deepcopy(var.attrs) for name, var in data.variables.items()}
            if store:
                with store.get() as current:
                    attrs = {**current.attrs, **attrs}
                    variable_attrs = {
                        name: {**var.attrs, **variable_attrs.get(name, {})}
                        for name, var in current.variables.items()
                    } | {name: values for name, values in variable_attrs.items() if name not in current}
            if operation == "upsert" and not store:
                store.insert(data)
            else:
                getattr(store, operation)(data)
            self._set_attributes(attrs, variable_attrs)

        self._mutate(action)

    def append(self, data: xr.Dataset) -> None:
        """Insert new timestamps; any overlap raises and discards this edit.

        Incoming attributes override matching attributes, preserving unmentioned
        global and variable attributes. Existing index options define overlaps.
        """
        self._write("insert", data)

    def update(self, data: xr.Dataset) -> None:
        """Replace values at existing timestamps, including supplied NaNs.

        Unknown timestamps raise and discard this edit. Omitted timestamps are
        preserved. Attribute merging follows :meth:`append`.
        """
        self._write("update", data)

    def upsert(self, data: xr.Dataset) -> None:
        """Update matching timestamps and insert new ones in the working data.

        Supplied NaNs replace existing values; omitted timestamps are preserved.
        Attribute merging follows :meth:`append`.
        """
        self._write("upsert", data)

    def replace(self, data: xr.Dataset) -> None:
        """Replace the entire working dataset, including its attributes.

        A fresh payload allows input to depend lazily on the previous working
        dataset. Previous working previews may become unreadable afterwards.
        """

        def action() -> None:
            self._validate(data)
            replacement = self._datasource._begin_payload_edit(None)
            try:
                replacement.store.insert(data)
            except BaseException:
                replacement.abort()
                raise
            previous = self._payload
            self._payload = replacement
            previous.abort()

        self._mutate(action, replaces_payload=True)

    def _set_attributes(self, attrs: dict, variable_attrs: dict[str, dict]) -> None:
        import zarr

        store = self._payload.store
        group = zarr.open_group(store.store, mode="a")
        group.attrs.update(attrs)
        for name, values in variable_attrs.items():
            group[name].attrs.update(values)
        if store._zarr_format == 2:
            zarr.consolidate_metadata(store.store)

    def update_attributes(
        self,
        data_vars: str | list[str] | None = None,
        update_global: bool = True,
        to_update: dict | None = None,
        to_delete: str | list[str] | None = None,
    ) -> None:
        """Edit global or variable attributes in the working version only.

        Args:
            data_vars: Variables to edit in addition to global attributes.
            update_global: Whether to edit global dataset attributes.
            to_update: Attribute values to add or replace.
            to_delete: Attribute names to remove. Missing names raise KeyError.

        Missing variables raise KeyError. An empty request leaves the editor
        unchanged. Searchable datasource descriptors remain owned by DataManager.
        """
        if not (to_update or to_delete) or (not update_global and data_vars is None):
            self._check_open()
            return

        def action() -> None:
            import zarr

            store = self._payload.store
            if not store:
                raise ValueError("Cannot edit attributes of an empty working dataset.")
            group = zarr.open_group(store.store, mode="a")
            names = [data_vars] if isinstance(data_vars, str) else data_vars or []
            targets = ([group.attrs] if update_global else []) + [group[name].attrs for name in names]
            keys = [to_delete] if isinstance(to_delete, str) else to_delete or []
            for attrs in targets:
                for key in keys:
                    attrs.pop(key)
                attrs.update(to_update or {})
            if store._zarr_format == 2:
                zarr.consolidate_metadata(store.store)

        self._mutate(action)

    def get_data(self) -> xr.Dataset:
        """Return a lazy working preview, valid until the next mutation or abort.

        A preview is not an immutable snapshot while edits continue. After a
        successful commit it refers to that committed payload. Closing the
        returned dataset does not close its datasource or manager.
        """
        self._check_open()
        self._previewed = True
        return self._payload.store.get()

    def commit(self, message: str = "") -> str:
        """Publish all staged changes as one new immutable version and close.

        Args:
            message: Description stored with this version's parent and timestamp.

        Returns:
            The new public version label, such as ``"v2"``.

        Raises:
            ValueError: If no changes have been requested or the message is not a string.
            ObjectStoreError: If publication detects a stale writer. Reload the
                datasource before retrying any failed publication.

        The first successful commit opts the datasource into immutable editing.
        Legacy mutation flags then raise rather than change saved versions.
        Identical-value rewrites still count as changes; no data hashing is used.
        """
        from openghg.util._time import get_representative_daterange_str, split_daterange_str, timestamp_now

        try:
            self._check_open()
            if not isinstance(message, str):
                raise ValueError("The commit message must be a string.")
            if not self._changed:
                raise ValueError("There are no staged changes to commit.")
            datasource = self._datasource
            state = datasource._edit_state()
            next_version = max(
                state.get("_next_version", 1),
                max((int(v[1:]) for v in state["_data_keys"]), default=0) + 1,
            )
            version = f"v{next_version}"
            timestamp = str(timestamp_now())
            with self._payload.store.get() as data:
                date_range = get_representative_daterange_str(data, period=state["_metadata"].get("period"))
            start, end = split_daterange_str(date_range)
            state["_data_keys"][version] = [date_range]
            state["_timestamps"][version] = timestamp
            state.update(
                _latest_version=version,
                _last_updated=timestamp,
                _versioning_policy="immutable",
                _next_version=next_version + 1,
            )
            state.setdefault("_commits", {})[version] = {
                "message": message,
                "parent": self.base,
                "timestamp": timestamp,
            }
            state["_metadata"].update(
                latest_version=version,
                timestamp=timestamp,
                start_date=str(start),
                end_date=str(end),
                versions=deepcopy(state["_data_keys"]),
            )
            self._payload.finish()
            datasource._publish_edit(self._payload, state, version)
        except BaseException:
            self.abort()
            raise
        self._close()
        return version
