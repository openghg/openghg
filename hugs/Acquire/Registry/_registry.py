
__all__ = ["Registry"]

_registry_key = "registry"


def _inc_uid(vals):
    import copy as _copy
    vals = _copy.copy(vals)

    for j in range(0, len(vals)):
        i = len(vals) - j - 1
        vals[i] += 1

        if i % 2 == 1:
            if vals[i] < 10:
                return vals
            else:
                vals[i] = 0
        else:
            if vals[i] < 52:
                return vals
            else:
                vals[i] = 0

    # we only get here if we have overflowed. In this
    # case, add an extra pair of digits
    vals = [0] * (len(vals) + 2)

    return vals


def _to_uid(vals):
    import string as _string
    parts = []
    for i in range(0, len(vals)):
        x = vals[i]
        if i % 2 == 1:
            if x < 0 or x > 9:
                raise ValueError(x)
            else:
                parts.append(str(x))
        else:
            if x < 0 or x > 51:
                raise ValueError(x)
            elif x < 26:
                parts.append(_string.ascii_lowercase[x])
            else:
                parts.append(_string.ascii_uppercase[x-26])

    s = "".join(parts)
    return ".".join([s[i:i+2] for i in range(0, len(s), 2)])


def _generate_service_uid(bucket, registry_uid):
    """Function to generate a new service_uid on this registry.

       The UIDs have the form a0-a0, when "a" is any letter from [a-zA-Z]
       and "0" is any number from [0-9]. This give 520 possible values
       for each part either side of the hyphen.

       The part on the left of the hypen is the root UID, which
       matches the root of the service_uid of the registry service
       that registered this service (the service_uid of a registry
       service has the UID root-root).

       If more than 520 values are needed, then either side of the
       ID can be extended by additional pairs of a0 digits, using
       a "." to separate pairs, e.g.

       the service_uid for registry b4-b4 that comes after
       b4-Z9.Z9.Z9 is b4-a0.a0.a0.a0

       similarly, the registry after Z9 is A0-A0.

       This means that

       a0.a0-a0.a0.a0.a0

       would be a perfectly valid ID. We would only need IDs of this
       length if we have ~270k registry services, and this service_uid
       came from a service that had registered ~73 trillion services...

       The registry root Z9, with registry Z9-Z9 is reserved for
       the temporary registry created during testing
    """
    from Acquire.ObjectStore import ObjectStore as _ObjectStore
    from Acquire.ObjectStore import Mutex as _Mutex

    root = registry_uid.split("-")[0]

    key = "%s/last_service_uid" % _registry_key

    mutex = _Mutex(key=key)

    try:
        last_vals = _ObjectStore.get_object_from_json(bucket=bucket,
                                                      key=key)
        last_vals = _inc_uid(last_vals)
    except:
        last_vals = [0, 0]

    service_uid = "%s-%s" % (root, _to_uid(last_vals))

    while service_uid == registry_uid:
        last_vals = _inc_uid(last_vals)
        service_uid = "%s-%s" % (root, _to_uid(last_vals))

    _ObjectStore.set_object_from_json(bucket=bucket, key=key, data=last_vals)
    mutex.unlock()

    return service_uid


class Registry:
    """This class holds the registry of all services registered by
       this service. Registries provided trusted actors who
       can supply public keys, URLs, and UIDs for all of the different
       services in the system.
    """
    def __init__(self):
        """Constructor"""
        self._bucket = None

        from Acquire.Service import get_this_service as _get_this_service
        self._registry_uid = _get_this_service(
                                need_private_access=False).uid()

    def get_registry_uid(self, service_uid):
        """Return the UID of the registry that initially registered
           the service with UID 'service_uid'.
        """
        parts = service_uid.split("-")
        return "%s-%s" % (parts[0], parts[0])

    def get_bucket(self):
        if self._bucket:
            return self._bucket
        else:
            from Acquire.Service import get_service_account_bucket \
                as _get_service_account_bucket
            self._bucket = _get_service_account_bucket()
            return self._bucket

    def registry_root(self):
        """Return the root ID of this registry"""
        return self._registry_uid.split("-")[0]

    def registry_uid(self):
        """Return the UID of this registry"""
        return self._registry_uid

    def is_test_registry(self):
        """Return whether or not this is a test registry (only used
           while testing the code)
        """
        return self._registry_uid == "Z9-Z9"

    def _get_key_for_uid(self, service_uid):
        return "%s/uid/%s" % (_registry_key, service_uid)

    def _get_uid_from_key(self, key):
        return key.split("/")[-1]

    def _get_domain(self, url):
        """Return the domain name of the server in the passed url"""
        from urllib.parse import urlparse as _urlparse
        try:
            domain = _urlparse(url).netloc.split(":")[0]
        except:
            domain = None

        if domain is None or len(domain) == 0:
            if self.is_test_registry():
                return "testing"
            else:
                raise ValueError("Cannot extract the domain from '%s'"
                                 % url)

        return domain

    def _get_root_key_for_domain(self, domain):
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded

        return "%s/domain/%s" % (_registry_key, _string_to_encoded(domain))

    def _get_key_for_url(self, service_url):
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        return "%s/url/%s" % (_registry_key, _string_to_encoded(service_url))

    def get_service_key(self, service_uid=None, service_url=None):
        """Return the key for the passed service in the object store"""
        if service_uid is not None:
            return self._get_key_for_uid(service_uid)
        else:
            bucket = self.get_bucket()
            key = self._get_key_for_url(service_url)

            try:
                from Acquire.ObjectStore import ObjectStore as _ObjectStore
                service_key = _ObjectStore.get_string_object(bucket=bucket,
                                                             key=key)
            except:
                service_key = None

            return service_key

    def challenge_service(self, service):
        """Send a challenge to the passed service, returning the actual
           service returned. This will only pass if our copy of the
           service matches us with the copy returned from the actual
           service. This verifies that there is a real service sitting
           at that URL, and that we have the right keys and certs
        """
        from Acquire.Crypto import PrivateKey as _PrivateKey
        from Acquire.ObjectStore import bytes_to_string as _bytes_to_string
        from Acquire.Service import Service as _Service

        challenge = _PrivateKey.random_passphrase()
        pubkey = service.public_key()
        encrypted_challenge = pubkey.encrypt(challenge)

        args = {"challenge": _bytes_to_string(encrypted_challenge),
                "fingerprint": pubkey.fingerprint()}

        if service.uid().startswith("STAGE"):
            # we cannot call using the service directly, as it is
            # not yet fully constructed
            from Acquire.Service import get_this_service as _get_this_service
            from Acquire.Service import call_function as _call_function
            this_service = _get_this_service(need_private_access=True)
            result = _call_function(service_url=service.service_url(),
                                    function=None,
                                    args=args,
                                    args_key=service.public_key(),
                                    response_key=this_service.private_key(),
                                    public_cert=service.public_certificate())
        else:
            result = service.call_function(function=None, args=args)

        if result["response"] != challenge:
            raise PermissionError(
                "Failure of the service %s to correctly respond "
                "to the challenge!" % service)

        return _Service.from_data(result["service_info"])

    def get_service(self, service_uid=None, service_url=None):
        """Load and return the service with specified url or uid
           from the registry. This will consult with other
           registry services to find the matching service
        """
        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.Service import Service as _Service
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        from Acquire.Service import get_this_service as _get_this_service

        this_service = _get_this_service(need_private_access=False)

        if service_url is not None:
            from Acquire.Service import Service as _Service
            service_url = _Service.get_canonical_url(service_url)

        if this_service.uid() == service_uid:
            return this_service
        elif this_service.canonical_url() == service_url:
            return this_service

        bucket = self.get_bucket()

        service_key = self.get_service_key(service_uid=service_uid,
                                           service_url=service_url)

        service = None

        if service_key is not None:
            try:
                data = _ObjectStore.get_object_from_json(bucket=bucket,
                                                         key=service_key)
                service = _Service.from_data(data)
            except:
                pass

        if service is not None:
            must_write = False

            if service.uid() == "STAGE1":
                # we need to directly ask the service for its info
                service = self.challenge_service(service)

                if service.uid() == "STAGE1":
                    from Acquire.Service import MissingServiceError
                    raise MissingServiceError(
                        "Service %s|%s not available as it is still under "
                        "construction!" % (service_uid, service))

                # we can now move this service from pending to active
                uidkey = self._get_key_for_uid(service.uid())
                domain = self._get_domain(service.service_url())
                domainroot = self._get_root_key_for_domain(domain=domain)

                pending_key = "%s/pending/%s" % (domainroot, service.uid())
                active_key = "%s/active/%s" % (domainroot, service.uid())

                try:
                    _ObjectStore.delete_object(bucket=bucket,
                                               key=pending_key)
                except:
                    pass

                try:
                    _ObjectStore.set_string_object(bucket=bucket,
                                                   key=active_key,
                                                   string_data=uidkey)
                except:
                    pass

                must_write = True
            elif service.should_refresh_keys():
                service.refresh_keys()
                must_write = True

            if must_write:
                data = service.to_data()
                _ObjectStore.set_object_from_json(bucket=bucket,
                                                  key=service_key,
                                                  data=data)
            return service

        # we only get here if we can't find the service on this registry.
        # In the future, we will use the initial part of the UID of
        # the service to ask its registering registry for its data.
        # For now, we just raise an error
        from Acquire.Service import MissingServiceError
        raise MissingServiceError(
            "No service available: service_url=%s  service_uid=%s" %
                                  (service_url, service_uid))

    def register_service(self, service, force_new_uid=False):
        """Register the passed service"""
        from Acquire.Service import Service as _Service
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        if not isinstance(service, _Service):
            raise TypeError("You can only register Service objects")

        if service.uid() != "STAGE1":
            raise PermissionError("You cannot register a service twice!")

        # first, stop a single domain monopolising resources...
        bucket = self.get_bucket()
        domain = self._get_domain(service.service_url())
        domainroot = self._get_root_key_for_domain(domain=domain)

        try:
            pending_keys = _ObjectStore.get_all_object_names(
                                        bucket=bucket,
                                        prefix="%s/pending/" % domainroot)
            num_pending = len(pending_keys)
        except:
            num_pending = 0

        if num_pending >= 4:
            raise PermissionError(
                "You cannot register a new service as you have reached "
                "the quota (4) for the number of pending services registered "
                "against the domain '%s'. Please get some of these services "
                "so that you can make them active." % domain)

        try:
            active_keys = _ObjectStore.get_all_object_names(
                                        bucket=bucket,
                                        prefix="%s/active/" % domainroot)
            num_active = len(active_keys)
        except:
            num_active = 0

        if num_active + num_pending >= 16:
            raise PermissionError(
                "You cannot register a new service as you have reached "
                "the quota (16) for the number registered against the "
                "domain '%s'" % domain)

        # first, challenge the service to ensure that it exists
        # and our keys are correct
        service = self.challenge_service(service)

        if service.uid() != "STAGE1":
            raise PermissionError("You cannot register a service twice!")

        bucket = self.get_bucket()
        urlkey = self._get_key_for_url(service.canonical_url())

        try:
            uidkey = _ObjectStore.get_string_object(bucket=bucket,
                                                    key=urlkey)
        except:
            uidkey = None

        service_uid = None

        if uidkey is not None:
            # there is already a service registered at this domain. Since
            # we have successfully challenged the service, this must be
            # someone re-bootstrapping a service. It is safe to give them
            # back their UID if requested
            if not force_new_uid:
                service_uid = self._get_uid_from_key(uidkey)

        if service_uid is None:
            # how many services from this domain are still pending?

            service_uid = _generate_service_uid(
                                        bucket=self.get_bucket(),
                                        registry_uid=self.registry_uid())

        # save this service to the object store
        uidkey = self._get_key_for_uid(service_uid)

        _ObjectStore.set_object_from_json(bucket=bucket, key=uidkey,
                                          data=service.to_data())

        _ObjectStore.set_string_object(bucket=bucket, key=urlkey,
                                       string_data=uidkey)

        domainkey = self._get_root_key_for_domain(domain=domain)

        _ObjectStore.set_string_object(
                                bucket=bucket,
                                key="%s/pending/%s" % (domainkey, service_uid),
                                string_data=uidkey)

        return service_uid

    def _get_key_for_uid(self, service_uid):
        return "%s/uid/%s" % (_registry_key, service_uid)

    def _get_key_for_url(self, service_url):
        from Acquire.ObjectStore import string_to_encoded \
            as _string_to_encoded
        return "%s/url/%s" % (_registry_key, _string_to_encoded(service_url))
