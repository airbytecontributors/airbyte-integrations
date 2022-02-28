#
# Copyright (c) 2021 Airbyte, Inc., all rights reserved.
#

import abc
import os
import time
from pathlib import Path
from typing import Any, Callable

import airbyte_api_client
import yaml
from airbyte_api_client.api import destination_api, source_api
from airbyte_api_client.model.destination_create import DestinationCreate
from airbyte_api_client.model.destination_search import DestinationSearch
from airbyte_api_client.model.destination_update import DestinationUpdate
from airbyte_api_client.model.source_create import SourceCreate
from airbyte_api_client.model.source_search import SourceSearch
from airbyte_api_client.model.source_update import SourceUpdate
from click import ClickException

from .diff_helpers import compute_checksum, compute_diff


class DuplicateRessourceError(ClickException):
    pass


class InvalidConfigurationError(ClickException):
    pass


class ResourceState:
    def __init__(self, configuration_path, resource_id, generation_timestamp, configuration_checksum):
        self.configuration_path = configuration_path
        self.resource_id = resource_id
        self.generation_timestamp = generation_timestamp
        self.configuration_checksum = configuration_checksum
        self.path = os.path.join(os.path.dirname(self.configuration_path), "state.yaml")

    def _save(self):
        content = {
            "configuration_path": self.configuration_path,
            "resource_id": self.resource_id,
            "generation_timestamp": self.generation_timestamp,
            "configuration_checksum": self.configuration_checksum,
        }
        with open(self.path, "w") as state_file:
            yaml.dump(content, state_file)

    @classmethod
    def create(cls, configuration_path, resource_id):
        generation_timestamp = int(time.time())
        configuration_checksum = compute_checksum(configuration_path)
        state = ResourceState(configuration_path, resource_id, generation_timestamp, configuration_checksum)
        state._save()
        return state

    @classmethod
    def from_file(cls, file_path):
        with open(file_path, "r") as f:
            raw_state = yaml.load(f, yaml.FullLoader)
        return ResourceState(
            raw_state["configuration_path"],
            raw_state["resource_id"],
            raw_state["generation_timestamp"],
            raw_state["configuration_checksum"],
        )


class BaseAirbyteResource(abc.ABC):
    @property
    @abc.abstractmethod
    def api(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def create_function_name(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def update_function_name(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def search_function_name(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def create_payload(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def search_payload(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def update_payload(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def resource_id_field(
        self,
    ):  # pragma: no cover
        pass

    @property
    @abc.abstractmethod
    def resource_type(
        self,
    ):  # pragma: no cover
        pass

    @property
    def _create_fn(self) -> Callable:
        return getattr(self.api, self.create_function_name)

    @property
    def _update_fn(self) -> Callable:
        return getattr(self.api, self.update_function_name)

    @property
    def _search_fn(self) -> Callable:
        return getattr(self.api, self.search_function_name)

    def __init__(self, api_client: airbyte_api_client.ApiClient, workspace_id, local_configuration: dict, configuration_path: str) -> None:
        self.workspace_id = workspace_id
        self.local_configuration = local_configuration
        self.configuration_path = configuration_path
        self.api_instance = self.api(api_client)
        self.state = self.get_state()
        self.remote_resource = self.get_remote_resource()
        self.was_created = True if self.remote_resource else False
        self.local_file_changed = (
            True if self.state is None else compute_checksum(self.configuration_path) != self.state.configuration_checksum
        )

    def get_state(self):
        expected_state_path = Path(os.path.join(os.path.dirname(self.configuration_path), "state.yaml"))
        if expected_state_path.is_file():
            return ResourceState.from_file(expected_state_path)

    def get_connection_configuration_diff(self):
        current_config = self.configuration
        if self.was_created:
            remote_config = self.remote_resource.connection_configuration
        diff = compute_diff(remote_config, current_config)
        return diff.pretty()

    def __getattr__(self, name: str) -> Any:
        """Map attribute of the YAML config to the BaseAirbyteResource object.

        Args:
            name (str): Attribute name

        Raises:
            AttributeError: Raised if the attributed was not found in the API response payload.

        Returns:
            [Any]: Attribute value
        """
        if name in self.local_configuration:
            return self.local_configuration.get(name)
        raise AttributeError(f"{self.__class__.__name__}.{name} is invalid.")

    def _create_or_update(self, operation_fn, payload):
        try:
            result = operation_fn(self.api_instance, payload)
            return result, ResourceState.create(self.configuration_path, result[self.resource_id_field])
        except airbyte_api_client.ApiException as e:
            if e.status == 422:
                # This error is really verbose from the API response, but it embodies all the details about why the config is not valid.
                # TODO alafanechere: try to parse it and display it in a more readable way.
                raise InvalidConfigurationError(e.body)
            else:
                raise e

    def create(self):
        return self._create_or_update(self._create_fn, self.create_payload)

    def update(self):
        return self._create_or_update(self._update_fn, self.update_payload)

    def _search(self):
        return self._search_fn(self.api_instance, self.search_payload)

    def get_remote_resource(self):
        search_results = self._search().get(f"{self.resource_type}s", [])
        if len(search_results) > 1:
            raise DuplicateRessourceError("Two or more ressource exist with the same name")
        if len(search_results) == 1:
            return search_results[0]
        else:
            return None

    @property
    def resource_id(self):
        return self.remote_resource.get(self.resource_id_field)


class Source(BaseAirbyteResource):

    api = source_api.SourceApi
    create_function_name = "create_source"
    resource_id_field = "source_id"
    search_function_name = "search_sources"
    update_function_name = "update_source"
    resource_type = "source"

    @property
    def create_payload(self):
        return SourceCreate(self.definition_id, self.configuration, self.workspace_id, self.resource_name)

    @property
    def search_payload(self):
        if self.state is None:
            return SourceSearch(source_definition_id=self.definition_id, workspace_id=self.workspace_id, name=self.resource_name)
        else:
            return SourceSearch(source_definition_id=self.definition_id, workspace_id=self.workspace_id, source_id=self.state.resource_id)

    @property
    def update_payload(self):
        return SourceUpdate(
            source_id=self.resource_id,
            connection_configuration=self.configuration,
            name=self.resource_name,
        )


class Destination(BaseAirbyteResource):
    api = destination_api.DestinationApi
    create_function_name = "create_destination"
    resource_id_field = "destination_id"
    search_function_name = "search_destinations"
    update_function_name = "update_destination"
    resource_type = "destination"

    @property
    def create_payload(self):
        return DestinationCreate(self.workspace_id, self.resource_name, self.definition_id, self.configuration)

    @property
    def search_payload(self):
        if self.state is None:
            return DestinationSearch(destination_definition_id=self.definition_id, workspace_id=self.workspace_id, name=self.resource_name)
        else:
            return DestinationSearch(
                destination_definition_id=self.definition_id, workspace_id=self.workspace_id, destination_id=self.state.resource_id
            )

    @property
    def update_payload(self):
        return DestinationUpdate(
            destination_id=self.resource_id,
            connection_configuration=self.configuration,
            name=self.resource_name,
        )


def factory(api_client, workspace_id, configuration_path):
    with open(configuration_path, "r") as f:
        local_configuration = yaml.load(f, yaml.FullLoader)
    if local_configuration["definition_type"] == "source":
        AirbyteResource = Source
    if local_configuration["definition_type"] == "destination":
        AirbyteResource = Destination
    return AirbyteResource(api_client, workspace_id, local_configuration, configuration_path)
