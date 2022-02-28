#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

from glob import glob
from typing import List

import click
from octavia_cli.check_context import REQUIRED_PROJECT_DIRECTORIES, requires_init

from .airbyte_resources import BaseAirbyteResource
from .airbyte_resources import factory as airbyte_resource_factory


@click.command(name="apply", help="Create an Airbyte resources from a YAML definition")
@click.option("--file", "-f", "configurations_files", type=click.Path(), multiple=True)
@click.option("--force", is_flag=True, default=False, help="Does not display the diff and updates without user prompt.")
@click.pass_context
@requires_init
def apply(ctx: click.Context, configurations_files: List[click.Path], force: bool):
    if not configurations_files:
        configurations_files = find_local_configuration_files()
    if not configurations_files:
        click.echo(click.style("😒 - No YAML file found to run apply.", fg="red"))
    for path in configurations_files:
        resource = airbyte_resource_factory(ctx.obj["API_CLIENT"], ctx.obj["WORKSPACE_ID"], path)
        apply_single_resource(resource, force)


def apply_single_resource(resource, force):
    if resource.was_created:
        click.echo(
            click.style(f"🐙 - {resource.resource_name} exists on your Airbyte instance, let's check if we need to update it!", fg="yellow")
        )
        messages = update_resource(resource, force)
    else:
        click.echo(click.style(f"🐙 - {resource.resource_name} does not exists on your Airbyte instance, let's create it!", fg="green"))
        messages = create_resource(resource)
    click.echo("\n".join(messages))


def should_update_resource(diff, local_file_changed, force):
    if force:
        should_update, update_reason = True, "🚨 - Run update because force mode is on."
    elif diff:
        should_update, update_reason = True, "✍️ - Run update because a diff was detected between local and remote resource."
    elif local_file_changed:
        should_update, update_reason = True, "✍️ - Run update because a local file change was detected and a secret field has been edited."
    else:
        should_update, update_reason = False, "😴 - Did not update because no change detected."
    return should_update, click.style(update_reason, fg="green")


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


def prompt_for_diff_validation(resource_name, diff):
    click.echo(
        click.style("👀 - Here's the computed diff (🚨 remind that diff on secret fields are not displayed):", fg="magenta", bold=True)
    )
    for line in diff.split("\n"):
        display_diff_line(line)
    return click.confirm(click.style(f"❓ - Do you want to update {resource_name} ?", bold=True))


def update_resource(airbyte_resource: BaseAirbyteResource, force: bool):
    diff = airbyte_resource.get_connection_configuration_diff()
    should_update, update_reason = should_update_resource(diff, airbyte_resource.local_file_changed, force)
    output_messages = [update_reason]
    if not force and diff:
        should_update = prompt_for_diff_validation(airbyte_resource.resource_name, diff)
    if should_update:
        updated_resource, state = airbyte_resource.update()
        output_messages.append(
            click.style(f"🎉 - Successfully updated {updated_resource.name} on your Airbyte instance!", fg="green", bold=True)
        )
        output_messages.append(click.style(f"💾 - New state for {updated_resource.name} stored at {state.path}.", fg="yellow"))
    return output_messages


def find_local_configuration_files():
    configuration_files = []
    for resource_directory in REQUIRED_PROJECT_DIRECTORIES:
        configuration_files += glob(f"./{resource_directory}/**/configuration.yaml")
    return configuration_files


def create_resource(airbyte_resource: BaseAirbyteResource):
    created_resource, state = airbyte_resource.create()
    return [
        click.style(f"🎉 - Successfully created {created_resource.name} on your Airbyte instance!", fg="green", bold=True),
        click.style(f"💾 - New state for {created_resource.name} stored at {state.path}.", fg="yellow"),
    ]


def display_diff(diff: str):
    for line in diff.split("\n"):
        display_diff_line(line)
