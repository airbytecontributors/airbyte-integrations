#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import click
from octavia_cli.check_context import ProjectNotInitializedError
from .airbyte_resources import BaseAirbyteResource, factory as airbyte_resource_factory


    

@click.command(name="apply", help="Create an Airbyte resources from a YAML definition")
@click.argument("yaml_file_path", type=click.Path())
@click.option("-y", "force", is_flag=True, default=False, help="Does not display the diff and updates without user prompt.")
@click.pass_context
def apply(ctx: click.Context, yaml_file_path: str, force: bool):
    if not ctx.obj["PROJECT_IS_INITIALIZED"]:
        raise ProjectNotInitializedError(
            "Your octavia project is not initialized, please run 'octavia init' before running 'octavia apply'."
        )

    airbyte_resource = airbyte_resource_factory(ctx.obj["API_CLIENT"], ctx.obj["WORKSPACE_ID"], yaml_file_path)
    if airbyte_resource.exists:
        click.echo(click.style("🐙 - This resource exists on your Airbyte instance, let's update it!", fg="yellow"))
        output = update_resource(airbyte_resource, force)
    else:
        click.echo(click.style("🐙 - This resource does not exists on your Airbyte instance, let's create it!", fg="green"))
        output = create_resource(airbyte_resource)
    click.echo(output)


def update_resource(airbyte_resource: BaseAirbyteResource, force: bool):
    if force:
        click.echo(click.style("Force mode is on, update without prompt!", fg="red", bold=True))
    else:
        click.echo(click.style("Let's compute the update diff", fg="green"))
        diff = airbyte_resource.get_connection_configuration_diff()
        display_diff(diff)
    if force or (diff and click.confirm(click.style(f"Do you want to update {airbyte_resource.resource_name} ?", bold=True))):
        return airbyte_resource.update()   

def create_resource(airbyte_resource: BaseAirbyteResource):
        return airbyte_resource.create()

def display_diff_line(diff_line: str):
    if "changed from" in diff_line:
        color = "yellow"
    elif "added" in diff_line:
        color = "green"
    elif "removed" in diff_line:
        color = "red"
    else:
        color = None
    click.echo(click.style(diff_line, fg=color))
    
def display_diff(diff: str):
    if not diff:
        click.echo(click.style("No change detected", fg="magenta"))
    else:
        for line in diff.split("/n"): 
            display_diff_line(line)