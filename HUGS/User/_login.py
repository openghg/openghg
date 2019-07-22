__all__ = ["login"]


def login(username):
    """ User login using the Acquire login service

        Args:
            username (str): Username for login
        Returns:
            dict: Dictionary containing login_url, session_uid, short_uid
    """
    from Acquire.Client import User as _User

    identity_url = "https://hugs.acquire-aaai.com/t/identity"

    user = _User(username=username, identity_url=identity_url)

    return user.request_login()

