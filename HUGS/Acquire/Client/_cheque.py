
import datetime as _datetime
import json as _json

from ._errors import PaymentError

__all__ = ["Cheque"]


class Cheque:
    """This class acts like a real world cheque, except it can only
       be used to transfer money from a regular user to a service
       user's account. This allows users to pay for services. This can be
       written by a user against one of their accounts, to be sent to a
       recipient service to pay for a named service. The recipient can
       send the cheque to the accounting service to trigger payment,
       upon which a CreditNote will be returned. Once receipted,
       payment will be complete.
    """

    def __init__(self):
        self._cheque = None
        self._accounting_service_url = None

    @staticmethod
    def write(account=None, resource=None,
              recipient_url=None, max_spend=None,
              expiry_date=None):
        """Create and return a cheque that can be used at any point
           in the future to authorise a transaction. If 'recipient_url'
           is supplied, then only the service with the matching
           URL can 'cash' the cheque (it will need to sign the cheque
           before sending it to the accounting service). If 'max_spend'
           is specified, then the cheque is only valid up to that
           maximum spend. Otherwise, it is valid up to the maximum
           daily spend limit (or other limits) of the account. If
           'expiry_date' is supplied then this cheque is valid only
           before the supplied datetime. If 'resource' is
           supplied then this cheque is only valid to pay for the
           specified resource (this should be a string that everyone
           agrees represents the resource in question). Note that
           this cheque is for a future transaction. We do not check
           to see if there are sufficient funds now, and this does
           not affect the account. If there are insufficient funds
           when the cheque is cashed (or it breaks spending limits)
           then the cheque will bounce.

           Args:
                account (Account, default=None): Account to use to write
                cheque
                resource (str, default=None): Define the resource to pay for
                recipient_url (str, default=None): URL of service to use
                max_spend (Decimal, default=None): Limit of cheque
                expiry_date (datetime, default=None): Cheque's expiry date
        """

        from Acquire.Client import Account as _Account

        if not isinstance(account, _Account):
            raise TypeError("You must pass a valid Acquire.Client.Account "
                            "object to write a cheque...")

        if max_spend is not None:
            from Acquire.ObjectStore import decimal_to_string \
                as _decimal_to_string
            max_spend = _decimal_to_string(max_spend)

        if expiry_date is not None:
            from Acquire.ObjectStore import datetime_to_string \
                as _datetime_to_string
            expiry_date = _datetime_to_string(expiry_date)

        if recipient_url is not None:
            recipient_url = str(recipient_url)

        from Acquire.ObjectStore import create_uuid as _create_uuid
        from Acquire.Identity import Authorisation as _Authorisation

        info = _json.dumps({"recipient_url": recipient_url,
                            "max_spend": max_spend,
                            "expiry_date": expiry_date,
                            "uid": _create_uuid(),
                            "resource": str(resource),
                            "account_uid": account.uid()})

        auth = _Authorisation(user=account.user(), resource=info)

        data = {"info": info, "authorisation": auth.to_data()}

        cheque = Cheque()

        cheque._cheque = account.accounting_service().encrypt_data(data)
        cheque._accounting_service_url = \
            account.accounting_service().canonical_url()

        return cheque

    def read(self, spend, resource, receipt_by):
        """Read the cheque - this will read the cheque to return the
           decrypted contents. This will only work if this function
           is called on the accounting service that will cash the
           cheque, if the signature on the cheque matches the
           service that is authorised to cash the cheque, and
           if the passed resource matches the resource
           encoded in the cheque. If this is all correct, then the
           returned dictionary will contain;

           {"recipient_url": The URL of the service which was sent the cheque,
            "recipient_key_fingerprint": Verified fingerprint of the service
                                         key that signed this cheque
            "spend": The amount authorised by this cheque,
            "uid": The unique ID for this cheque,
            "resource": String that identifies the resource this cheque will
                        be used to pay for,
            "account_uid": UID of the account from which funds will be drawn
            "authorisation" : Verified authorisation from the user who
                              says they own the account for the spend
            "receipt_by" : Time when we must receipt the cheque, or
                           we will lose the money
           }

           You must pass in the spend you want to draw from the cheque,
           a string representing the resource this cheque will
           be used to pay for, and the time by which you promise to receipt
           the cheque after cashing

           Args:
                spend (Decimal): Amount authorised by cheque
                resource (str): Resource to pay for
                receipt_by (datetime): Time cheque must be receipted
                by
           Returns:
                dict: Dictionary described above

        """

        if self._cheque is None:
            raise PaymentError("You cannot read a null cheque")

        from Acquire.ObjectStore import string_to_decimal \
            as _string_to_decimal
        from Acquire.ObjectStore import string_to_datetime \
            as _string_to_datetime
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.Service import get_this_service as _get_this_service

        spend = _string_to_decimal(spend)
        resource = str(resource)
        receipt_by = _string_to_datetime(receipt_by)

        service = _get_this_service(need_private_access=True)

        # get the cheque data - this may have been signed
        try:
            cheque_data = _json.loads(self._cheque["signed_data"])
        except:
            cheque_data = self._cheque

        # decrypt the cheque's data - only possible on the accounting service
        cheque_data = service.decrypt_data(cheque_data)

        # the date comprises the user-authorisation that acts as a
        # signature that the user wrote this cheque, and the info
        # for the cheque to say how it is valid
        from Acquire.Identity import Authorisation as _Authorisation
        auth = _Authorisation.from_data(cheque_data["authorisation"])
        info = cheque_data["info"]

        # the json.dumps version is the resource used to verify
        # the above authorisation
        auth_resource = info

        # validate that the user authorised this cheque
        try:
            auth.verify(resource=info)
        except Exception as e:
            raise PaymentError(
                "The user's signature/authorisation for this cheque "
                "is not valid! ERROR: %s" % str(e))

        info = _json.loads(info)

        # the user signed this cheque :-)
        info["authorisation"] = auth

        # check the signature if one was needed
        try:
            recipient_url = info["recipient_url"]
        except:
            recipient_url = None

        if recipient_url:
            # the recipient was specified - verify that we trust
            # the recipient, and that they have signed the cheque
            recipient_service = service.get_trusted_service(
                                            service_url=recipient_url)
            recipient_service.verify_data(self._cheque)
            info["recipient_key_fingerprint"] = self._cheque["fingerprint"]

        # validate that the item signature is correct
        try:
            cheque_resource = info["resource"]
        except:
            cheque_resource = None

        if cheque_resource is not None:
            if resource != resource:
                raise PaymentError(
                    "Disagreement over the resource for which "
                    "this cheque has been signed")

        info["resource"] = resource
        info["auth_resource"] = auth_resource

        try:
            max_spend = info["max_spend"]
            del info["max_spend"]
        except:
            max_spend = None

        if max_spend is not None:
            max_spend = _string_to_decimal(max_spend)

            if max_spend < spend:
                raise PaymentError(
                    "The requested spend (%s) exceeds the authorised "
                    "maximum value of the cheque" % (spend))

        info["spend"] = spend

        try:
            expiry_date = info["expiry_date"]
            del expiry_date["expiry_date"]
        except:
            expiry_date = None

        if expiry_date is not None:
            expiry_date = _string_to_datetime(expiry_date)

            # validate that the cheque will not have expired
            # when we receipt it
            from Acquire.ObjectStore import get_datetime_now \
                as _get_datetime_now
            now = _get_datetime_now()

            if now > receipt_by:
                raise PaymentError(
                    "The time when you promised to receipt the cheque "
                    "has already passed!")

            if receipt_by > expiry_date:
                raise PaymentError(
                    "The cheque will have expired after you plan to "
                    "receipt it!: %s versus %s" %
                    (_datetime_to_string(receipt_by),
                     _datetime_to_string(expiry_date)))

        info["receipt_by"] = receipt_by

        return info

    def cash(self, spend, resource, receipt_within=3600):
        """Cash this cheque, specifying how much to be cashed,
           and the resource that will be paid for
           using this cheque. This will send the cheque to the
           accounting service (if we trust that accounting service).
           The accounting service will check that the cheque is valid,
           and the signature of the item is correct. It will then
           withdraw 'spend' funds from the account that signed the
           cheque, returning valid CreditNote(s) that can be trusted
           to show that the funds exist.

           If 'receipt_within' is set, then the CreditNotes will
           be automatically cancelled if they are not
           receipted within 'receipt_within' seconds

           It is your responsibility to receipt the note for
           the actual valid incurred once the service has been
           delivered, thereby actually transferring the cheque
           funds into your account (on that accounting service)

           This returns a list of the CreditNote(s) that were
           cashed from the cheque

           Args:
                spend (Decimal): Value to withdraw
                resource (str): Resource to spend value on
                receipt_within (datetime, default=3600): Time to receipt
                the cashing of this cheque by
           Returns:
                list: List of CreditNotes

        """
        if self._cheque is None:
            raise PaymentError("You cannot cash a null cheque!")

        # sign the cheque to show we have seen it
        from Acquire.Service import get_this_service as _get_this_service
        service = _get_this_service(need_private_access=True)
        self._cheque = service.sign_data(self._cheque)

        # get the trusted accounting service that will honour the cheque
        accounting_service = self.accounting_service()

        # when do we guarantee to receipt the credit notes by?
        from Acquire.ObjectStore import get_datetime_future \
            as _get_datetime_future
        receipt_by = _get_datetime_future(receipt_within)

        # which account should the money be paid into?
        account_uid = service.service_user_account_uid(
                                accounting_service=accounting_service)

        # next - send the cheque to the accounting service to
        # show that we know the item_id and want to cash it
        from Acquire.ObjectStore import decimal_to_string \
            as _decimal_to_string
        from Acquire.ObjectStore import datetime_to_string \
            as _datetime_to_string
        from Acquire.ObjectStore import string_to_list \
            as _string_to_list

        result = accounting_service.call_function(
            function="cash_cheque",
            args={"cheque": self.to_data(),
                  "spend": _decimal_to_string(spend),
                  "resource": str(resource),
                  "account_uid": account_uid,
                  "receipt_by": _datetime_to_string(receipt_by)})

        credit_notes = None

        try:
            from Acquire.Accounting import CreditNote as _CreditNote
            credit_notes = _string_to_list(result["credit_notes"],
                                           _CreditNote)
        except Exception as e:
            raise PaymentError(
                "Attempt to cash the cheque has not resulted in a "
                "valid CreditNote? Error = %s" % str(e))

        total_cashed = 0

        for note in credit_notes:
            total_cashed = total_cashed + note.value()
            if note.account_uid() != account_uid:
                raise PaymentError(
                    "The cashed cheque is paying into the wrong account! "
                    "%s. It should be going to %s" %
                    (note.account_uid(), account_uid))

        if total_cashed != spend:
            raise PaymentError(
                "The value of the cheque (%s) does not match the total value "
                "of the credit note(s) returned (%s)" % (spend, total_cashed))

        return credit_notes

    def accounting_service_url(self):
        """Return the URL of the accounting service that will honour
           this cheque

           Returns:
                str: URL of accounting service
        """
        return self._accounting_service_url

    def accounting_service(self):
        """Return the accounting service that will honour this cheque.
           Note that this will only return the service if it is trusted
           by the service on which this function is called

           Returns:
                Service: Trusted accounting service

        """
        from Acquire.Service import get_this_service as _get_this_service
        service = _get_this_service()
        accounting_service = service.get_trusted_service(
                                            self.accounting_service_url())

        if not accounting_service.is_accounting_service():
            from Acquire.Service import ServiceError
            raise ServiceError(
                "The service that is supposed to honour the cheque (%s) "
                "does not appear to be a valid accounting service" %
                (str(accounting_service)))

        return accounting_service

    def to_data(self):
        """Return a JSON-serialisable dictionary of this object

           Returns:
                dict: JSON serialisable dictionary of this object
        """
        data = {}

        if self._cheque is not None:
            data["accounting_service_url"] = self._accounting_service_url
            data["cheque"] = self._cheque

        return data

    @staticmethod
    def from_data(data):
        """Return a cheque constructed from the passed (JSON-deserialised
           dictionary)

           Args:
                data (dict): Dictionary from which to create object
           Returns:
                Cheque: Cheque object created from JSON data

        """
        cheque = Cheque()

        if (data and len(data) > 0):
            cheque._cheque = data["cheque"]
            cheque._accounting_service_url = data["accounting_service_url"]

        return cheque
