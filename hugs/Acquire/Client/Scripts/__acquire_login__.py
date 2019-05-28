#!/bin/env python3


def main():
    import argparse
    import sys

    from Acquire.Client import Wallet, LoginError

    parser = argparse.ArgumentParser(
                    description="Log into an Acquire-based identity "
                                "service via a login url",
                    prog="acquire_login")

    parser.add_argument("url", type=str, nargs="*",
                        help="Login URL")

    parser.add_argument('-u', '--username', type=str, nargs='?',
                        help="Username with which to log in")

    parser.add_argument('--remember-password', action="store_true",
                        default=True,
                        help="Remember the password (default on)")

    parser.add_argument('--remember-device', action="store_true",
                        default=None,
                        help="Remember this device (saves OTP code, "
                             "default off)")

    parser.add_argument('--no-remember-device', action="store_true",
                        default=None,
                        help="Don't remember this device, and don't ask to")

    parser.add_argument('--no-remember-password', action="store_true",
                        default=None,
                        help="Don't remember the password, and don't ask to")

    parser.add_argument('--remove-service', type=str, nargs="*",
                        help="Remove locally stored information about the "
                        "passed service(s)")

    parser.add_argument('--dry-run', action="store_true", default=None,
                        help="Do a dry-run of the login - don't connect to "
                        "the server")

    args = parser.parse_args()

    remember_device = args.remember_device

    if args.no_remember_device:
        remember_device = False

    remember_password = args.remember_password

    if remember_password is None:
        remember_password = True

    if args.no_remember_password:
        remember_password = False

    dryrun = args.dry_run

    if not remember_password:
        # should not remember the otpsecret if
        # we don't trust this to remember the password!
        remember_device = False

    do_nothing = True

    wallet = Wallet()

    if args.remove_service:
        for service in args.remove_service:
            try:
                do_nothing = False
                print("Removing locally stored information "
                      "about service '%s'" % service)
                wallet.remove_service(service)
            except Exception as e:
                print(e)
                pass

    if do_nothing and len(args.url) == 0:
        parser.print_help(sys.stdout)

    if len(args.url) == 0:
        sys.exit(0)

    for url in args.url:
        try:
            wallet.send_password(url=url, username=args.username,
                                 remember_password=remember_password,
                                 remember_device=remember_device,
                                 dryrun=dryrun)
        except LoginError as e:
            print("\n%s" % e.args)
        except Exception as e:
            from Acquire.Service import exception_to_string
            print(exception_to_string(e))

if __name__ == "__main__":
    main()
