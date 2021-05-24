#
# MIT License
#
# Copyright (c) 2020 Airbyte
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#

from __future__ import annotations
from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Type

import requests
from airbyte_cdk.models import SyncMode
from airbyte_cdk.sources.streams.http import HttpStream


class AsanaStream(HttpStream, ABC):
    url_base = "https://app.asana.com/api/1.0/"

    primary_key = "gid"

    # Asana pagination could be from 1 to 100.
    page_size = 100

    parent = None
    template_path = None

    def path(self, stream_slice: Mapping[str, Any] = None, **kwargs) -> str:
        return self.template_path.format(**stream_slice)

    def backoff_time(self, response: requests.Response) -> Optional[int]:
        delay_time = response.headers.get("Retry-After")
        if delay_time:
            return int(delay_time)

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        decoded_response = response.json()
        next_page = decoded_response.get("next_page")
        if next_page:
            return {"offset": next_page["offset"]}

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:

        params = {"limit": self.page_size}

        params.update(self.get_opt_fields())

        if next_page_token:
            params.update(next_page_token)

        return params

    def get_opt_fields(self) -> MutableMapping[str, str]:
        """
        For "GET all" request for almost each stream Asana API by default returns 3 fields for each
        record: `gid`, `name`, `resource_type`. Since we want to get all fields we need to specify those fields in each
        request. For each stream set of fields will be different and we get those fields from stream's schema.
        Also each nested object, like `workspace`, or list of nested objects, like `followers`, also by default returns
        those 3 fields mentioned above, so for nested stuff we also need to specify fields we want to return and we
        decided that for all nested objects and list of objects we will be getting only `gid` field.
        Plus each stream can have it's exceptions about how request required fields, like in `Tasks` stream.
        More info can be found here - https://developers.asana.com/docs/input-output-options.
        """
        opt_fields = list()
        schema = self.get_json_schema()

        for prop, value in schema["properties"].items():
            if "object" in value["type"]:
                opt_fields.append(self._handle_object_type(prop, value))
            elif "array" in value["type"]:
                opt_fields.append(self._handle_array_type(prop, value.get("items", [])))
            else:
                opt_fields.append(prop)

        return {"opt_fields": ",".join(opt_fields)} if opt_fields else dict()

    def _handle_object_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        return f"{prop}.gid"

    def _handle_array_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        if "type" in value and "object" in value["type"]:
            return self._handle_object_type(prop, value)

        return prop

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        response_json = response.json()
        yield from response_json.get("data", [])  # Asana puts records in a container array "data"

    def read_stream(self, sync_mode=SyncMode.full_refresh) -> Iterable[Mapping]:
        """
        General function for getting parent stream (which should be passed through `stream_class`) slice.
        Generates dicts with `gid` of parent streams.
        """
        stream_slices = self.stream_slices(sync_mode=sync_mode)
        for stream_slice in stream_slices:
            for record in self.read_records(sync_mode=sync_mode, stream_slice=stream_slice):
                yield record

    def stream_slices(
        self,
        sync_mode: SyncMode,
        cursor_field: List[str] = None,
        stream_state: Mapping[str, Any] = None
    ) -> Iterable[Optional[Mapping[str, Any]]]:
        if self.parent:
            stream = self.parent(authenticator=self.authenticator)
            for record in stream.read_stream():
                yield {"parent_id": record[self.parent.primary_key]}


class Workspaces(AsanaStream):
    template_path = "workspaces"


class WorkspaceRequestParamsRelatedStream(AsanaStream, ABC):
    """
    Few streams (Projects, Tags and Users) require passing `workspace` as request argument.
    So this is basically the whole point of this class - to pass `workspace` as request argument.
    """
    parent = Workspaces

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["workspace"] = stream_slice["parent_id"]
        return params


class Projects(WorkspaceRequestParamsRelatedStream):
    template_path = "projects"


class CustomFields(AsanaStream):
    parent = Workspaces
    template_path = "workspaces/{parent_id}/custom_fields"


class Sections(AsanaStream):
    parent = Projects
    template_path = "projects/{parent_id}/sections"


class Tasks(AsanaStream):
    parent = Projects
    template_path = "tasks"

    def request_params(
        self,
        stream_state: Mapping[str, Any],
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        params = super().request_params(stream_state=stream_state, stream_slice=stream_slice, next_page_token=next_page_token)
        params["project"] = stream_slice["parent_id"]
        return params

    def _handle_object_type(self, prop: str, value: dict) -> str:
        if prop == "custom_fields":
            return prop
        elif prop in ("hearts", "likes"):
            return f"{prop}.user.gid"
        elif prop == "memberships":
            return "memberships.(project|section).gid"

        return f"{prop}.gid"


class Stories(AsanaStream):
    parent = Tasks
    template_path = "tasks/{parent_id}/stories"


class Tags(WorkspaceRequestParamsRelatedStream):
    template_path = "tags"


class Teams(AsanaStream):
    parent = Workspaces
    template_path = "organizations/{parent_id}/teams"


class TeamMemberships(AsanaStream):
    parent = Teams
    template_path = "teams/{parent_id}/team_memberships"


class Users(WorkspaceRequestParamsRelatedStream):
    template_path = "users"

    def _handle_object_type(self, prop: str, value: MutableMapping[str, Any]) -> str:
        if prop == "photo":
            return prop

        return f"{prop}.gid"
