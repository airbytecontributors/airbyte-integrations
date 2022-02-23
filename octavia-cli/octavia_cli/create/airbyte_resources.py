import abc

import airbyte_api_client
from airbyte_api_client.api import source_api, destination_api
from airbyte_api_client.model.source_create import SourceCreate
from airbyte_api_client.model.destination_create import DestinationCreate

from typing import Callable, Any
import yaml

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
    def create_payload(
        self,
    ):  # pragma: no cover
        pass

    @property
    def _create_fn(self) -> Callable:
        return getattr(self.api, self.create_function_name)


    def __init__(self, api_client: airbyte_api_client.ApiClient, workspace_id, yaml_config: dict) -> None:
        self.workspace_id = workspace_id
        self._yaml_config = yaml_config
        self.api_instance = self.api(api_client)

    def __getattr__(self, name: str) -> Any:
        """Map attribute of the YAML config to the BaseAirbyteResource object.

        Args:
            name (str): Attribute name

        Raises:
            AttributeError: Raised if the attributed was not found in the API response payload.

        Returns:
            [Any]: Attribute value
        """
        if name in self._yaml_config:
            return self._yaml_config.get(name)
        raise AttributeError(f"{self.__class__.__name__}.{name} is invalid.")


    def create(self):
        return self._create_fn(self.api_instance, self.create_payload)


class Source(BaseAirbyteResource):
    api = source_api.SourceApi
    create_function_name = "create_source"

    @property
    def create_payload(self):
        return SourceCreate(
            self.definition_id,
            self.configuration,
            self.workspace_id,
            self.resource_name
        )

class Destination(BaseAirbyteResource):
    api = destination_api.DestinationApi
    create_function_name = "create_destination"

    @property
    def create_payload(self):
        return DestinationCreate(
            self.definition_id,
            self.configuration,
            self.workspace_id,
            self.resource_name
        )

def factory(api_client, workspace_id, yaml_file_path):
    with open(yaml_file_path, "r") as f:
        yaml_config = yaml.load(f, yaml.FullLoader)
    if yaml_config["definition_type"] == "source":
        AirbyteResource = Source
    if yaml_config["definition_type"] == "destination":
        AirbyteResource = Destination
    return AirbyteResource(api_client, workspace_id, yaml_config)