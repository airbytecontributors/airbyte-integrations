from abc import ABC, abstractmethod


class TestContext(BaseModel):
    local_root: str = Field(description="The filesystem path where test files can be placed.")


class DestinationAcceptanceTestHelper(ABC):
    @abstractmethod
    def retrieve_records(self, stream_name: str, namespace: Optional[str], stream_schema: Mapping[str, Any]) -> Iterable[Mapping[str, Any]]:
        """
        Implement this method to return all of the records in destination as json at the time this method is
        invoked. These will be used to check that the data actually written is what should actually be
        there.
        """

    def run(self, args: List[str]):
        pass # TODO
