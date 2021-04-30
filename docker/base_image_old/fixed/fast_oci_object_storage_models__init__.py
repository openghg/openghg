# coding: utf-8
# Copyright (c) 2016, 2018, Oracle and/or its affiliates. All rights reserved.

from __future__ import absolute_import

import lazy_import as _lazy_import

Bucket = _lazy_import.lazy_class("oci.object_storage.models.bucket.Bucket")
BucketSummary = _lazy_import.lazy_class("oci.object_storage.models.bucket_summary.BucketSummary")
CommitMultipartUploadDetails = _lazy_import.lazy_class("oci.object_storage.models.commit_multipart_upload_details.CommitMultipartUploadDetails")
CommitMultipartUploadPartDetails = _lazy_import.lazy_class("oci.object_storage.models.commit_multipart_upload_part_details.CommitMultipartUploadPartDetails")
CopyObjectDetails = _lazy_import.lazy_class("oci.object_storage.models.copy_object_details.CopyObjectDetails")
CreateBucketDetails = _lazy_import.lazy_class("oci.object_storage.models.create_bucket_details.CreateBucketDetails")
CreateMultipartUploadDetails = _lazy_import.lazy_class("oci.object_storage.models.create_multipart_upload_details.CreateMultipartUploadDetails")
CreatePreauthenticatedRequestDetails = _lazy_import.lazy_class("oci.object_storage.models.create_preauthenticated_request_details.CreatePreauthenticatedRequestDetails")
ListObjects = _lazy_import.lazy_class("oci.object_storage.models.list_objects.ListObjects")
MultipartUpload = _lazy_import.lazy_class("oci.object_storage.models.multipart_upload.MultipartUpload")
MultipartUploadPartSummary = _lazy_import.lazy_class("oci.object_storage.models.multipart_upload_part_summary.MultipartUploadPartSummary")
NamespaceMetadata = _lazy_import.lazy_class("oci.object_storage.models.namespace_metadata.NamespaceMetadata")
ObjectLifecyclePolicy = _lazy_import.lazy_class("oci.object_storage.models.object_lifecycle_policy.ObjectLifecyclePolicy")
ObjectLifecycleRule = _lazy_import.lazy_class("oci.object_storage.models.object_lifecycle_rule.ObjectLifecycleRule")
ObjectNameFilter = _lazy_import.lazy_class("oci.object_storage.models.object_name_filter.ObjectNameFilter")
ObjectSummary = _lazy_import.lazy_class("oci.object_storage.models.object_summary.ObjectSummary")
PreauthenticatedRequest = _lazy_import.lazy_class("oci.object_storage.models.preauthenticated_request.PreauthenticatedRequest")
PreauthenticatedRequestSummary = _lazy_import.lazy_class("oci.object_storage.models.preauthenticated_request_summary.PreauthenticatedRequestSummary")
PutObjectLifecyclePolicyDetails = _lazy_import.lazy_class("oci.object_storage.models.put_object_lifecycle_policy_details.PutObjectLifecyclePolicyDetails")
RenameObjectDetails = _lazy_import.lazy_class("oci.object_storage.models.rename_object_details.RenameObjectDetails")
RestoreObjectsDetails = _lazy_import.lazy_class("oci.object_storage.models.restore_objects_details.RestoreObjectsDetails")
UpdateBucketDetails = _lazy_import.lazy_class("oci.object_storage.models.update_bucket_details.UpdateBucketDetails")
UpdateNamespaceMetadataDetails = _lazy_import.lazy_class("oci.object_storage.models.update_namespace_metadata_details.UpdateNamespaceMetadataDetails")
WorkRequest = _lazy_import.lazy_class("oci.object_storage.models.work_request.WorkRequest")
WorkRequestError = _lazy_import.lazy_class("oci.object_storage.models.work_request_error.WorkRequestError")
WorkRequestLogEntry = _lazy_import.lazy_class("oci.object_storage.models.work_request_log_entry.WorkRequestLogEntry")
WorkRequestResource = _lazy_import.lazy_class("oci.object_storage.models.work_request_resource.WorkRequestResource")
WorkRequestSummary = _lazy_import.lazy_class("oci.object_storage.models.work_request_summary.WorkRequestSummary")

# Maps type names to classes for object_storage services.
object_storage_type_mapping = {
    "Bucket": Bucket,
    "BucketSummary": BucketSummary,
    "CommitMultipartUploadDetails": CommitMultipartUploadDetails,
    "CommitMultipartUploadPartDetails": CommitMultipartUploadPartDetails,
    "CopyObjectDetails": CopyObjectDetails,
    "CreateBucketDetails": CreateBucketDetails,
    "CreateMultipartUploadDetails": CreateMultipartUploadDetails,
    "CreatePreauthenticatedRequestDetails": CreatePreauthenticatedRequestDetails,
    "ListObjects": ListObjects,
    "MultipartUpload": MultipartUpload,
    "MultipartUploadPartSummary": MultipartUploadPartSummary,
    "NamespaceMetadata": NamespaceMetadata,
    "ObjectLifecyclePolicy": ObjectLifecyclePolicy,
    "ObjectLifecycleRule": ObjectLifecycleRule,
    "ObjectNameFilter": ObjectNameFilter,
    "ObjectSummary": ObjectSummary,
    "PreauthenticatedRequest": PreauthenticatedRequest,
    "PreauthenticatedRequestSummary": PreauthenticatedRequestSummary,
    "PutObjectLifecyclePolicyDetails": PutObjectLifecyclePolicyDetails,
    "RenameObjectDetails": RenameObjectDetails,
    "RestoreObjectsDetails": RestoreObjectsDetails,
    "UpdateBucketDetails": UpdateBucketDetails,
    "UpdateNamespaceMetadataDetails": UpdateNamespaceMetadataDetails,
    "WorkRequest": WorkRequest,
    "WorkRequestError": WorkRequestError,
    "WorkRequestLogEntry": WorkRequestLogEntry,
    "WorkRequestResource": WorkRequestResource,
    "WorkRequestSummary": WorkRequestSummary
}
