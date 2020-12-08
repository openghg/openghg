import argparse
import os
import shutil
import subprocess

parser = argparse.ArgumentParser(description='Build the base Docker image and optionally push to DockerHub')
parser.add_argument('tag', metavar='tag', type=str, nargs='+', help='tag name/number')
parser.add_argument('--push', dest='push', action='store_true', default=False, help='push the image to DockerHub')
parser.add_argument('--no-cleanup', dest='cleanup', action='store_false', default=True, help='delete copied files after run')

args = parser.parse_args()

# We want the latest requirements file for OpenGHG
shutil.copy("../../requirements.txt", "requirements.txt")
shutil.copy("../../requirements-server.txt", "requirements-server.txt")
# A tag for the image
tag_str = ":".join(("openghg/openghg-base", str(args.tag[0])))

subprocess.run(["docker", "build", "--tag", tag_str, "."])

if args.push:
    subprocess.run(["docker", "push", tag_str])

if args.cleanup:
    os.remove("requirements.txt")
    os.remove("requirements-server.txt")
