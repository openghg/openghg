# from typing import Dict
# from Acquire.Client import Wallet


# def call_test_service() -> str:
#     service_url = "https://fn.openghg.org/t"

#     wallet = Wallet()
#     cloud_service = wallet.get_service(service_url=f"{service_url}/openghg")

#     args = {"test": "test"}
#     response: Dict = cloud_service.call_function(function="testing.test_connection", args=args)
#     to_return: str = response["results"]

#     return to_return
