from jobio.job import submit
from jobio.cli.args import add_execute_group, add_s3_group, add_storage_group


def add_job_cli(parser):
    job_commands = parser.add_subparsers(title="Commands")
    run_parser = job_commands.add_parser("run")
    add_execute_group(run_parser)
    add_storage_group(run_parser)
    add_s3_group(run_parser)
    run_parser.set_defaults(func=submit)
