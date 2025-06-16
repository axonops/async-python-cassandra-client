from cassandra.policies import RetryPolicy

from ...config import config


class LimitedRetryPolicy(RetryPolicy):
    max_attempts = config.cassandra_max_attempts

    def on_read_timeout(
        self, query, consistency, required_responses, received_responses, data_retrieved, retry_num
    ):
        if retry_num >= self.max_attempts:
            return self.RETHROW, None
        return self.RETRY, consistency

    def on_write_timeout(
        self, query, consistency, write_type, required_responses, received_responses, retry_num
    ):
        if retry_num >= self.max_attempts:
            return self.RETHROW, None
        return self.RETRY, consistency

    def on_unavailable(self, query, consistency, required_replicas, alive_replicas, retry_num):
        if retry_num >= self.max_attempts:
            return self.RETHROW, None
        return self.RETRY, consistency

    def on_request_error(self, query, consistency, error, retry_num):
        if retry_num >= self.max_attempts:
            return self.RETHROW, None
        return self.RETRY, consistency
