# coding: utf-8
# Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.

from __future__ import absolute_import

import lazy_import as _lazy_import

MultipartObjectAssembler = _lazy_import.lazy_class("oci.object_storage.transfer.internal.multipart_object_assembler.MultipartObjectAssembler")
models = _lazy_import.lazy_module("oci.object_storage.models")
ObjectStorageClient = _lazy_import.lazy_class("oci.object_storage.object_storage_client.ObjectStorageClient")
ObjectStorageClientCompositeOperations = _lazy_import.lazy_class("oci.object_storage.object_storage_client_composite_operations.ObjectStorageClientCompositeOperations")
UploadManager = _lazy_import.lazy_class("oci.object_storage.transfer.upload_manager.UploadManager")

__all__ = ["ObjectStorageClient", "ObjectStorageClientCompositeOperations", "models", "MultipartObjectAssembler",
           "UploadManager"]
