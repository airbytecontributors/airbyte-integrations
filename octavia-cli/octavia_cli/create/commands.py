#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import click
from octavia_cli.check_context import ProjectNotInitializedError
from .airbyte_resources import factory as airbyte_resource_factory
@click.command(name="create", help="Create an Airbyte resources from a YAML definition")
@click.argument("yaml_file_path", type=click.Path())
@click.pass_context
def create(ctx: click.Context, yaml_file_path: str):
    if not ctx.obj["PROJECT_IS_INITIALIZED"]:
        raise ProjectNotInitializedError(
            "Your octavia project is not initialized, please run 'octavia init' before running 'octavia create'."
        )

    airbyte_resource = airbyte_resource_factory(ctx.obj["API_CLIENT"], ctx.obj["WORKSPACE_ID"], yaml_file_path)
    message = airbyte_resource.create()
    click.echo(click.style(message, fg="green"))
