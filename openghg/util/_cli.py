import argparse


def cli() -> None:
    from openghg.util import create_config, show_versions

    parser = argparse.ArgumentParser(
        prog="OpenGHG CLI",
        description="The OpenGHG Command Line Interface helps you get OpenGHG setup on your local machine.",
        epilog="Text at the bottom of help",
    )
    parser.add_argument(
        "--default-config", action="store_true", help="Get OpenGHG setup with default a configuration"
    )
    parser.add_argument("--quickstart", action="store_true", help="Run the quickstart setup process")
    parser.add_argument("--version", action="store_true", help="Print the version information about OpenGHG")

    args = parser.parse_args()

    if args.default_config:
        create_config(silent=True)
    elif args.quickstart:
        create_config()
    elif args.version:
        show_versions()
    else:
        parser.print_help()
