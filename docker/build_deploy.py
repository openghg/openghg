import argparse
import os
import shutil
import subprocess


def cleanup():
    shutil.rmtree("openghg")
    shutil.rmtree("openghg_services")
    os.remove("requirements-server.txt")


parser = argparse.ArgumentParser(description="Build the base Docker image and optionally push to DockerHub")
parser.add_argument(
    "--tag",
    dest="tag",
    default="latest",
    type=str,
    help="tag name/number, examples: 1.0 or latest. Not full tag name such as openghg/openghg-complete:latest. Default: latest",
)
parser.add_argument("--push", dest="push", action="store_true", default=False, help="push the image to DockerHub")
parser.add_argument(
    "--build", dest="build", action="store_true", default=False, help="build the docker image. Disables Fn deploy."
)
parser.add_argument("--deploy", dest="deploy", action="store_false", default=True, help="buid image and deploy the Fn functions")
parser.add_argument(
    "--build-base",
    dest="base",
    action="store_true",
    default=False,
    help="build the base docker image before building the complete image",
)

args = parser.parse_args()

# We want the latest requirements file for OpenGHG
try:
    shutil.copytree("../openghg", "openghg")
    shutil.copytree("../services", "openghg_services")
    shutil.copy("../requirements-server.txt", "requirements-server.txt")
except FileExistsError:
    pass

# A tag for the image
tag = args.tag
tag_str = ":".join(("openghg/openghg-complete", tag))

if args.build:
    args.deploy = False
    print("\nBuilding docker file...\n")
    subprocess.check_call(["docker", "build", "--tag", tag_str, "."])

if args.base:
    print("\nBuilding base docker image...\n")
    subprocess.check_call(["python3", "build.py", "--tag", tag], cwd="base_image")

if args.deploy:
    print("\nDeploying Fn functions...\n")
    # Make sure we have an app calld openghg
    subprocess.run(["fn", "create", "app", "openghg"])
    # Build and deploy the function container
    try:
        subprocess.check_call(["fn", "--verbose", "deploy", "--local"])
    except subprocess.CalledProcessError as e:
        cleanup()
        raise ValueError(
            (f"Error {e}.\n\nPlease make sure you've already built the base image "
            "(or pass --build-base to this script) and have Fn running\n\n"))

if args.push:
    subprocess.check_call(["docker", "push", tag_str])

cleanup()
