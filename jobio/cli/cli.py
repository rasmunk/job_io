import argparse
from jobio.defaults import PACKAGE_NAME
from jobio.cli.helpers import add_job_cli


def run():
    parser = argparse.ArgumentParser(prog=PACKAGE_NAME)
    add_job_cli(parser)

    args = parser.parse_args()
    # Execute default funciton
    if hasattr(args, "func"):
        result = args.func(args)
        if result:
            print(result)
    return None


if __name__ == "__main__":
    arguments = run()
