# coding: utf-8
# Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.

import lazy_import as _lazy_import

audit = _lazy_import.lazy_module("oci.audit")
container_engine = _lazy_import.lazy_module("oci.container_engine")
core = _lazy_import.lazy_module("oci.core")
database = _lazy_import.lazy_module("oci.database")
dns = _lazy_import.lazy_module("oci.dns")
email = _lazy_import.lazy_module("oci.email")
file_storage = _lazy_import.lazy_module("oci.file_storage")
identity = _lazy_import.lazy_module("oci.identity")
key_management = _lazy_import.lazy_module("oci.key_management")
load_balancer = _lazy_import.lazy_module("oci.load_balancer")
object_storage = _lazy_import.lazy_module("oci.object_storage")
resource_search = _lazy_import.lazy_module("oci.resource_search")

auth = _lazy_import.lazy_module("oci.auth")
config = _lazy_import.lazy_module("oci.config")
constants = _lazy_import.lazy_module("oci.constants")
decorators = _lazy_import.lazy_module("oci.decorators")
exceptions = _lazy_import.lazy_module("oci.exceptions")
regions = _lazy_import.lazy_module("oci.regions")
pagination = _lazy_import.lazy_module("oci.pagination")
retry = _lazy_import.lazy_module("retry")

BaseClient = _lazy_import.lazy_class("oci.base_client.BaseClient")
Request = _lazy_import.lazy_class("oci.request.Request")
Signer = _lazy_import.lazy_class("oci.signer.Signer")

# from .version import __version__  # noqa

wait_until = _lazy_import.lazy_function("oci.waiter.wait_until")

__all__ = [
    "BaseClient", "Error", "Request", "Response", "Signer", "config", "constants", 
    "decorators", "exceptions", "regions", "wait_until", "pagination", "auth", "retry",
    "audit", "container_engine", "core", "database", "dns", "email", "file_storage", 
    "identity", "key_management", "load_balancer", "object_storage", "resource_search"
]
