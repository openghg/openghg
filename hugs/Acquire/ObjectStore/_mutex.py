
import uuid
import datetime as _datetime
import time as _time

__all__ = ["Mutex"]


class Mutex:
    """This class implements a mutex that sits in the object store.
       The mutex is associated with a key. A thread holds this mutex
       if it has successfully written its secret to this key. If
       not, then another thread must hold the mutex, and we have
       to wait...
    """
    def __init__(self, key=None, timeout=10, lease_time=10, bucket=None):
        """Create the mutex. The immediately tries to lock the mutex
           for key 'key' and will block until a lock is successfully
           obtained (or until 'timeout' seconds has been reached, and an
           exception is then thrown). If the key is provided, then
           this is the (single) global mutex. Note that this is really
           a lease, as the mutex will only be held for a maximum of
           'lease_time' seconds. After this time the mutex will be
           automatically unlocked and made available to lock by
           others. You can renew the lease by re-locking the mutex.
        """
        if key is None:
            key = "mutexes/none"
        else:
            key = "mutexes/%s" % str(key).replace(" ", "_")

        if bucket is None:
            from Acquire.Service import get_service_account_bucket as \
                                       _get_service_account_bucket

            bucket = _get_service_account_bucket()

        self._bucket = bucket
        self._key = key
        self._secret = str(uuid.uuid4())
        self._is_locked = 0
        self.lock(timeout, lease_time)

    def __del__(self):
        """Release the mutex if it is held"""
        try:
            self.fully_unlock()
        except:
            pass

    def __str__(self):
        if self.expired():
            return "Mutex(%s, EXPIRED)" % self._key
        else:
            return "Mutex(%s, is_locked=%s)" % (self._key, self.is_locked())

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self._key == other._key and self._secret == other._secret

    def __ne__(self, other):
        return not self.__eq__(other)

    def is_locked(self):
        """Return whether or not this mutex is locked

           Returns:
                bool: True if mutex locked, else False
        """
        return self._is_locked > 0 and not self.expired()

    def seconds_remaining_on_lease(self):
        """Return the number of seconds remaining on this lease. You must
           unlock the mutex before the lease expires, or else an exception
           will be raised when you unlock, and you will likely have
           a race condition

           Returns:
                datetime: Time remaining on lease
        """
        if self.is_locked():
            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now
            now = _get_datetime_now()

            if self._end_lease > now:
                return (self._end_lease - now).seconds
            else:
                return 0
        else:
            return 0

    def expired(self):
        """Return whether or not this lock has expired

           Returns:
                bool: True if lock has expired, else False
        """
        if self._is_locked > 0:
            from Acquire.ObjectStore import get_datetime_now as \
                _get_datetime_now
            return self._end_lease < _get_datetime_now()
        else:
            return False

    def assert_not_expired(self):
        """Function that asserts that this mutex has not expired"""
        if self.expired():
            from Acquire.ObjectStore import MutexTimeoutError
            raise MutexTimeoutError("The lease on this mutex expired before "
                                    "this mutex was unlocked!")

    def fully_unlock(self):
        """This fully unlocks the mutex, removing all levels
           of recursion

           Returns:
                None
        """
        if self._is_locked == 0:
            return

        from Acquire.ObjectStore import ObjectStore as _ObjectStore
        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now

        try:
            holder = _ObjectStore.get_string_object(self._bucket, self._key)
        except:
            holder = None

        if holder == self._lockstring:
            # we hold the mutex - delete the key
            _ObjectStore.delete_object(self._bucket, self._key)

        self._lockstring = None
        self._is_locked = 0

        if self._end_lease < _get_datetime_now():
            self._end_lease = None
            from Acquire.ObjectStore import MutexTimeoutError
            raise MutexTimeoutError("The lease on this mutex expired before "
                                    "this mutex was unlocked!")
        else:
            self._end_lease = None

    def unlock(self):
        """Release the mutex if it is held. Does nothing if the mutex
           is not held. If the mutex is unlocked after the lease has
           expired then this will raise a MutexTimeoutError. You should
           check for this when you unlock to make sure that you
           have not risked a race condition.

           Returns:
                None
        """
        if self._is_locked == 0:
            return
        elif self._is_locked == 1:
            self.fully_unlock()
        else:
            self.assert_not_expired()
            self._is_locked -= 1

    def lock(self, timeout=None, lease_time=None):
        """Lock the mutex, blocking until the mutex is held, or until
           'timeout' seconds have passed. If we time out, then an exception is
           raised. The lock is held for a maximum of 'lease_time' seconds.

           Args:
                timeout (int): Number of seconds to block
                lease_time (int): Number of seconds to hold the lock
           Returns:
                None
        """
        # if the user does not provide a timeout, then we will set a timeout
        # to 10 seconds
        if timeout is None:
            timeout = 10.0
        else:
            timeout = float(timeout)

        # if the user does not provide a lease_time, then we will set a
        # default of only 10 seconds
        if lease_time is None:
            lease_time = 10.0
        else:
            lease_time = float(lease_time)

        from Acquire.ObjectStore import get_datetime_now as _get_datetime_now
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime
        from Acquire.ObjectStore import ObjectStore as _ObjectStore

        if self.is_locked():
            # renew the lease - if there is less than a second remaining
            # on the lease then unlock and then lock again from scratch
            now = _get_datetime_now()

            if (now > self._end_lease) or (now - self._end_lease).seconds < 1:
                self.fully_unlock()
                self.lock(timeout, lease_time)
            else:
                self._end_lease = now + _datetime.timedelta(seconds=lease_time)

                self._lockstring = "%s{}%s" % (
                    self._secret, _datetime_to_string(self._end_lease))

                _ObjectStore.set_string_object(self._bucket, self._key,
                                               self._lockstring)

                self._is_locked += 1

            return

        now = _get_datetime_now()
        endtime = now + _datetime.timedelta(seconds=timeout)

        # This is the first time we are trying to get a lock
        while now < endtime:
            # does anyone else hold the lock?
            try:
                holder = _ObjectStore.get_string_object(self._bucket,
                                                        self._key)
            except:
                holder = None

            is_held = True

            if holder is None:
                is_held = False
            else:
                end_lease = _string_to_datetime(holder.split("{}")[-1])
                if now > end_lease:
                    # the lease from the other holder has expired :-)
                    is_held = False

            if not is_held:
                # no-one holds this mutex - try to hold it now
                self._end_lease = now + _datetime.timedelta(seconds=lease_time)

                self._lockstring = "%s{}%s" % (
                    self._secret, _datetime_to_string(self._end_lease))

                _ObjectStore.set_string_object(self._bucket, self._key,
                                               self._lockstring)

                holder = _ObjectStore.get_string_object(self._bucket,
                                                        self._key)
            else:
                self._lockstring = None

            if holder == self._lockstring:
                # it looks like we are the holder - read and write again
                # just to make sure
                holder = _ObjectStore.get_string_object(self._bucket,
                                                        self._key)

                if holder == self._lockstring:
                    # write again just to make sure
                    _ObjectStore.set_string_object(self._bucket, self._key,
                                                   self._lockstring)

                    holder = _ObjectStore.get_string_object(self._bucket,
                                                            self._key)

            if holder == self._lockstring:
                # we have read and written our secret to the object store
                # three times. While a race condition is still possible,
                # I'd hope it is now highly unlikely - we now hold the mutex
                self._is_locked = 1
                return

            # only try the lock 4 times a second
            _time.sleep(0.25)

            now = _get_datetime_now()

        from Acquire.ObjectStore import MutexTimeoutError
        raise MutexTimeoutError("Cannot acquire a mutex lock on the "
                                "key '%s'" % self._key)
