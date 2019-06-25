
from cachetools import cached as _cached
from cachetools import LRUCache as _LRUCache

_cache_local_serviceinfo = _LRUCache(maxsize=5)
_cache_local_serviceinfos = _LRUCache(maxsize=5)

__all__ = ["get_trusted_service", "get_trusted_services",
           "clear_services_cache"]


def clear_services_cache():
    """Clear the caches of loaded services"""
    _cache_local_serviceinfo.clear()
    _cache_local_serviceinfos.clear()


# Cached as the list of all trusted remote service information
# will not change too often
@_cached(_cache_local_serviceinfos)
def get_trusted_services():
    """Return a dictionary of all trusted services indexed by
       their type
    """
    from Acquire.Service import is_running_service as _is_running_service

    if _is_running_service():
        from Acquire.Service import get_this_service as _get_this_service
        from Acquire.Service import Service as _Service

        from Acquire.Service import get_service_account_bucket as \
            _get_service_account_bucket
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import url_to_encoded as \
            _url_to_encoded

        # we already trust ourselves
        service = _get_this_service()

        trusted_services = {}
        trusted_services[service.service_type()] = [service]

        bucket = _get_service_account_bucket()

        uidkey = "_trusted/uid/"
        datas = _ObjectStore.get_all_objects(bucket, uidkey)

        for data in datas:
            remote_service = _Service.from_data(data)

            if remote_service.should_refresh_keys():
                # need to update the keys in our copy of the service
                remote_service.refresh_keys()
                key = "%s/%s" % (uidkey, remote_service.uid())
                _ObjectStore.set_object_from_json(bucket, key,
                                                  remote_service.to_data())

            if remote_service.service_type() in datas:
                datas[remote_service.service_type()].append(remote_service)
            else:
                datas[remote_service.service_type()] = [remote_service]

        return datas
    else:
        # this is running on the client
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        return wallet.get_services()


# Cached as the remote service information will not change too often
@_cached(_cache_local_serviceinfo)
def get_trusted_service(service_url=None, service_uid=None,
                        service_type=None, autofetch=True):
    """Return the trusted service info for the service with specified
       service_url or service_uid"""
    if service_url is not None:
        from Acquire.Service import Service as _Service
        service_url = _Service.get_canonical_url(service_url,
                                                 service_type=service_type)

    from Acquire.Service import is_running_service as _is_running_service

    if _is_running_service():
        from Acquire.Service import get_this_service as _get_this_service
        from Acquire.Service import Service as _Service
        from Acquire.Service import get_service_account_bucket as \
            _get_service_account_bucket
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import url_to_encoded as \
            _url_to_encoded

        service = _get_this_service()

        if service_url is not None and service.canonical_url() == service_url:
            # we trust ourselves :-)
            return service

        if service_uid is not None and service.uid() == service_uid:
            # we trust ourselves :-)
            return service

        bucket = _get_service_account_bucket()
        uidkey = None
        data = None

        if service_uid is not None:
            uidkey = "_trusted/uid/%s" % service_uid
            try:
                data = _ObjectStore.get_object_from_json(bucket, uidkey)
            except:
                pass
        elif service_url is not None:
            urlkey = "_trusted/url/%s" % _url_to_encoded(service_url)
            try:
                uidkey = _ObjectStore.get_string_object(bucket, urlkey)
                if uidkey is not None:
                    data = _ObjectStore.get_object_from_json(bucket, uidkey)
            except:
                pass

        if data is not None:
            remote_service = _Service.from_data(data)

            if remote_service.should_refresh_keys():
                # need to update the keys in our copy of the service
                remote_service.refresh_keys()

                if uidkey is not None:
                    _ObjectStore.set_object_from_json(
                                                    bucket, uidkey,
                                                    remote_service.to_data())

            return remote_service

        if not autofetch:
            from Acquire.Service import ServiceAccountError
            if service_uid is not None:
                raise ServiceAccountError(
                    "We do not trust the service with UID '%s'" %
                    service_uid)
            else:
                raise ServiceAccountError(
                    "We do not trust the service at URL '%s'" %
                    service_url)

        # we can try to fetch this data - we will ask our own
        # registry
        from Acquire.Registry import get_trusted_registry_service \
            as _get_trusted_registry_service
        registry = _get_trusted_registry_service(service_uid=service.uid())
        service = registry.get_service(service_uid=service_uid,
                                       service_url=service_url)

        from Acquire.Service import trust_service as _trust_service
        _trust_service(service)
        return service
    else:
        # this is running on the client
        from Acquire.Client import Wallet as _Wallet
        wallet = _Wallet()
        service = wallet.get_service(service_uid=service_uid,
                                     service_url=service_url,
                                     service_type=service_type,
                                     autofetch=autofetch)
        return service
