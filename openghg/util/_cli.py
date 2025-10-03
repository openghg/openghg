import argparse


def cli() -> None:
    from openghg.util import create_config, show_versions, handle_direct_store_path

    parser = argparse.ArgumentParser(
        prog="OpenGHG CLI",
        description="The OpenGHG Command Line Interface helps you get OpenGHG setup on your local machine.",
        epilog="Text at the bottom of help",
    )
    parser.add_argument(
        "--default-config", action="store_true", help="Get OpenGHG setup with default a configuration"
    )
    parser.add_argument("--quickstart", action="store_true", help="Run the quickstart setup process")
    parser.add_argument(
        "--register-store",
        nargs="+",
        metavar=("STORE_NAME", "STORE_PATH"),
        help=(
            "Register a new data store. "
            "You can specify just a path, or both a store name and path. "
            "Examples:\n"
            "  --register-store /path/to/store\n"
            "  --register-store my_store /path/to/store"
        ),
    )
    parser.add_argument("--version", action="store_true", help="Print the version information about OpenGHG")

    args = parser.parse_args()

    if args.default_config:
        create_config(silent=True)
    elif args.quickstart:
        create_config()
    elif args.version:
        show_versions()
    elif args.register_store:
        if len(args.register_store) == 1:
            store_name = None
            store_path = args.register_store[0]
        elif len(args.register_store) == 2:
            # Name and path provided
            store_name, store_path = args.register_store
        else:
            raise ValueError("Too many arguments for --register-store.")

        handle_direct_store_path(path=store_path, name=store_name, add_new_store=True)
    else:
        parser.print_help()
