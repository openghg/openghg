"""Copy encoded Zarr keys without decoding arrays or rewriting metadata."""

from __future__ import annotations

from typing import Literal, cast

from ._zarr_compat import ZarrStoreLike, zarr_has_async_store_api


def copy_zarr_store(
    source: ZarrStoreLike,
    dest: ZarrStoreLike,
    *,
    source_path: str = "",
    dest_path: str = "",
    if_exists: Literal["raise", "replace", "skip"] = "replace",
    dry_run: bool = False,
) -> tuple[int, int, int]:
    """Copy raw store keys, preserving encoded chunks and metadata exactly.

    Args:
        source: Store to read.
        dest: Store to write.
        source_path: Copy descendants of this slash-delimited path only.
            Paths normalize separators and reject ``.`` and ``..`` segments.
        dest_path: Prefix to add to copied keys after removing ``source_path``.
        if_exists: Raise on, replace, or skip existing destination keys.
            Destination-only keys are retained.
        dry_run: Count planned copies and skips without reading or writing payloads.

    Returns:
        Counts of copied keys, skipped keys, and bytes actually copied. During
        a dry run the first count is planned copies and the byte count is zero.

    Raises:
        ValueError: If ``if_exists`` or either path is invalid.
        FileExistsError: On a conflicting key with ``if_exists="raise"`` under
            Zarr 3. Zarr 2 raises its native ``zarr.errors.CopyError`` instead.

    Copying is not transactional: an error may leave partially copied keys.
    Callers requiring rollback must clean up their destination.
    """
    if if_exists not in ("raise", "replace", "skip"):
        raise ValueError("if_exists must be 'raise', 'replace', or 'skip'.")

    if not zarr_has_async_store_api():
        from zarr.convenience import copy_store

        result = copy_store(
            source,
            dest,
            source_path=source_path,
            dest_path=dest_path,
            if_exists=if_exists,
            dry_run=dry_run,
        )
        return cast(tuple[int, int, int], result)

    from zarr.core.sync import sync

    return cast(
        tuple[int, int, int], sync(_copy_async(source, dest, source_path, dest_path, if_exists, dry_run))
    )


async def _copy_async(
    source: ZarrStoreLike,
    dest: ZarrStoreLike,
    source_path: str,
    dest_path: str,
    if_exists: str,
    dry_run: bool,
) -> tuple[int, int, int]:
    from zarr.core.buffer import default_buffer_prototype
    from zarr.storage._common import normalize_path

    source_prefix = normalize_path(source_path)
    dest_prefix = normalize_path(dest_path)
    if source_prefix:
        source_prefix += "/"
    if dest_prefix:
        dest_prefix += "/"

    copied = skipped = nbytes = 0
    # Freeze the keys first so writes cannot extend a same-store listing.
    keys = sorted([key async for key in source.list() if key.startswith(source_prefix)])
    for source_key in keys:
        dest_key = dest_prefix + source_key[len(source_prefix) :]
        if if_exists != "replace" and await dest.exists(dest_key):
            if if_exists == "raise":
                raise FileExistsError(f"key {dest_key!r} exists in destination")
            skipped += 1
            continue
        if not dry_run:
            value = await source.get(source_key, prototype=default_buffer_prototype())
            if value is None:
                raise KeyError(source_key)
            await dest.set(dest_key, value)
            nbytes += len(value)
        copied += 1
    return copied, skipped, nbytes
