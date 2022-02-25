#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import click
from glob import glob
from octavia_cli.check_context import requires_init
from typing import List
from .airbyte_resources import BaseAirbyteResource
from .airbyte_resources import factory as airbyte_resource_factory


@click.command(name="apply", help="Create an Airbyte resources from a YAML definition")
@click.option("--file", "-f", "files", type=click.Path(), multiple=True)
@click.option("--force",  is_flag=True, default=False, help="Does not display the diff and updates without user prompt.")
@click.pass_context
@requires_init
def apply(ctx: click.Context, files: List[click.Path], force: bool):
    if not files:
        files = glob("./sources/*.yaml") + glob("./destinations/*.yaml") + glob("./connections/*.yaml")
    if not files:
        click.echo(click.style("😒 - No YAML file found to apply.", fg="red"))
    for path in files:
        resource = airbyte_resource_factory(ctx.obj["API_CLIENT"], ctx.obj["WORKSPACE_ID"], path)
        apply_single_resource(resource, force)

def apply_single_resource(resource, force):
    if resource.exists:
        click.echo(click.style(f"🐙 - {resource.resource_name} exists on your Airbyte instance, let's update it!", fg="yellow"))
        message = update_resource(resource, force)
    else:
        click.echo(click.style(f"🐙 - {resource.resource_name} does not exists on your Airbyte instance, let's create it!", fg="green"))
        message = create_resource(resource)
    click.echo(message)

def update_resource(airbyte_resource: BaseAirbyteResource, force: bool):
    if force:
        click.echo(click.style("🚨 - Force mode is on, update without prompt!", fg="red", bold=True))
    else:
        click.echo(click.style("👀 - Here's the computed diff:", fg="magenta", bold=True))
        diff = airbyte_resource.get_connection_configuration_diff()
        if not diff:
            return click.echo(click.style("\tNo change detected", fg="magenta"))
        else:
            display_diff(diff)
    if force or click.confirm(click.style(f"❓ - Do you want to update {airbyte_resource.resource_name} ?", bold=True)):
        updated_resource = airbyte_resource.update()
        return click.style(f"🎉 - Successfully updated {updated_resource.name} on your Airbyte instance!", fg="green", bold=True)

def create_resource(airbyte_resource: BaseAirbyteResource):
    created_resource = airbyte_resource.create()
    message = click.style(f"🎉 - Successfully created {created_resource.name} on your Airbyte instance!", fg="green", bold=True)
    return message

def display_diff_line(diff_line: str):
    if "changed from" in diff_line:
        color = "yellow"
        prefix = "E"
    elif "added" in diff_line:
        color = "green"
        prefix = "+"
    elif "removed" in diff_line:
        color = "red"
        prefix = "-"
    else:
        prefix = ""
        color = None
    click.echo(click.style(f"\t{prefix} - {diff_line}", fg=color))


def display_diff(diff: str):
    for line in diff.split("\n"):
        display_diff_line(line)

