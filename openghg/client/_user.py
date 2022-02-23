# from Acquire.Client import User


# def create_user() -> User:
#     from Acquire.Crypto import PrivateKey, OTP
#     from Acquire.Client import User, Wallet
#     from uuid import uuid4

#     username = str(uuid4())
#     password = PrivateKey.random_passphrase()

#     result = User.register(username=username, password=password, identity_url="identity")

#     otpsecret = result["otpsecret"]
#     otp = OTP(otpsecret)

#     # now log the user in
#     user = User(username=username, identity_url="identity", auto_logout=False)

#     result = user.request_login()

#     assert isinstance(result, dict)

#     wallet = Wallet()

#     wallet.send_password(
#         url=result["login_url"],
#         username=username,
#         password=password,
#         otpcode=otp.generate(),
#         remember_password=False,
#         remember_device=False,
#     )

#     user.wait_for_login()

#     assert user.is_logged_in()

#     return user
