from typing import Optional
from Acquire.Client import Wallet

__all__ = ["TestService"]


class TestService:
    def __init__(self, service_url: Optional[str] = None):
        if service_url is not None:
            self._service_url = service_url
        else:
            self._service_url = "https://fn.openghg.org/t"

        wallet = Wallet()
        self._service = wallet.get_service(service_url=f"{self._service_url}/openghg")

    def test(self) -> str:
        """Test connectivity to the OpenGHG Cloud Platform

        Returns:
            str: Timestamp of connection
        """
        if self._service is None:
            raise PermissionError("Cannot use a null service")

        args = {"test": "test"}
        response = self._service.call_function(function="testing.test_connection", args=args)
        to_return: str = response["results"]

        return to_return
