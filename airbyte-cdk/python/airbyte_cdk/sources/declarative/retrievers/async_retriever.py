# Copyright (c) 2024 Airbyte, Inc., all rights reserved.

from typing import Any, Iterable, Mapping, Optional, Callable

from airbyte_protocol.models import FailureType

from airbyte_cdk import AirbyteTracedException
from airbyte_cdk.sources.declarative.async_job.job_orchestrator import JobOrchestrator
from airbyte_cdk.sources.declarative.retrievers import Retriever
from airbyte_cdk.sources.declarative.stream_slicers import StreamSlicer
from airbyte_cdk.sources.declarative.types import StreamSlice, StreamState
from airbyte_cdk.sources.streams.core import StreamData


class AsyncRetriever(Retriever):
    def __init__(self, stream_slicer: StreamSlicer, job_orchestrator_factory: Callable[[Iterable[StreamSlice]], JobOrchestrator]) -> None:
        self._stream_slicer = stream_slicer
        self._job_orchestrator_factory = job_orchestrator_factory
        self.__job_orchestrator: Optional[JobOrchestrator] = None


    def stream_slices(self) -> Iterable[Optional[StreamSlice]]:
        self.__job_orchestrator = self._job_orchestrator_factory(self._stream_slicer.stream_slices())

        for completed_partition in self._job_orchestrator.create_and_get_completed_partitions():
            yield {"partition": completed_partition}

    def read_records(self, records_schema: Mapping[str, Any], stream_slice: Optional[StreamSlice] = None) -> Iterable[StreamData]:
        if not stream_slice or "partition" not in stream_slice:
            raise AirbyteTracedException(
                message="Invalid arguments to AsyncJobRetriever.read_records: stream_slice is no optional. Please contact Airbyte Support",
                failure_type=FailureType.system_error,
            )
        yield from self._job_orchestrator.fetch_records(stream_slice["partition"])

    @property
    def state(self) -> StreamState:
        """
        As a first iteration for sendgrid, there is no state to be managed
        """
        return {}

    @state.setter
    def state(self, value: StreamState) -> None:
        """
        As a first iteration for sendgrid, there is no state to be managed
        """
        pass

    @property
    def _job_orchestrator(self) -> JobOrchestrator:
        if self.__job_orchestrator:
            return self.__job_orchestrator

        raise AirbyteTracedException(
            message="Invalid state within AsyncJobRetriever. Please contact Airbyte Support",
            internal_message="AsyncPartitionRepository is expected to be accessed only after `stream_slices`",
            failure_type=FailureType.system_error,
        )