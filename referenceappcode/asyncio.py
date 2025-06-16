import asyncio
from asyncio import Future
from typing import List, Union

from cassandra.cluster import Session, ResponseFuture, _NOT_SET, EXEC_PROFILE_DEFAULT
from cassandra.query import PreparedStatement


class AsyncResultHandler:
    """
    This class receives asynchronous results from Cassandra queries and wraps the response in an asyncio.Future
    """
    fut: Future
    response_fut: ResponseFuture
    rows: List = []

    def __init__(self, response_fut: ResponseFuture):
        self.rows = []
        loop = asyncio.get_running_loop()
        self.fut = loop.create_future()
        self.response_fut = response_fut
        self.response_fut.add_callbacks(callback=self.handle_page, errback=self.handle_error)

    def handle_page(self, rows):
        self.rows.extend(rows)
        if self.response_fut.has_more_pages:
            self.response_fut.start_fetching_next_page()
        else:
            self.fut.get_loop().call_soon_threadsafe(self.fut.set_result, self.rows)

    def handle_error(self, exc: Exception):
        self.fut.get_loop().call_soon_threadsafe(self.fut.set_exception, exc)


def execute_async(session: Session,
                  statement: Union[str, PreparedStatement],
                  parameters=None,
                  trace=False,
                  custom_payload=None,
                  timeout=_NOT_SET,
                  execution_profile=EXEC_PROFILE_DEFAULT,
                  paging_state=None,
                  host=None,
                  execute_as=None) -> Future:
    """
    This function wraps session.execute_async() and returns an awaitable Future that will contain the list of rows
    in its result
    """
    return AsyncResultHandler(
        session.execute_async(
            statement,
            parameters,
            trace,
            custom_payload,
            timeout,
            execution_profile,
            paging_state,
            host,
            execute_as
        )
    ).fut
