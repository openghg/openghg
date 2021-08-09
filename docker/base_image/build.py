import argparse
import os
import shutil
import subprocess

parser = argparse.ArgumentParser(description="Build the base Docker image and optionally push to DockerHub")
parser.add_argument(
    "--tag",
    dest="tag",
    default="latest",
    type=str,
    help="tag name/number, examples: 1.0 or latest. Not full tag name such as openghg/openghg-base:latest. Default: latest",
)
parser.add_argument("--push", dest="push", action="store_true", default=False, help="push the image to DockerHub")
parser.add_argument("--no-cleanup", dest="cleanup", action="store_false", default=True, help="delete copied files after run")
parser.add_argument("--nocache", help="build image without using the cache", action="store_true")

args = parser.parse_args()

# We want the latest requirements file for OpenGHG
shutil.copy("../../requirements-server.txt", "requirements-server.txt")
# A tag for the image
tag_str = ":".join(("openghg/openghg-base", args.tag))

cmd_str = f"docker build --tag {tag_str} ."

if args.nocache:
    cmd_str += " --no-cache"

cmd_list = cmd_str.split()
print(cmd_list)
subprocess.check_call(cmd_list)

if args.push:
    subprocess.check_call(["docker", "push", tag_str])

if args.cleanup:
    os.remove("requirements-server.txt")
