"""
Logging implementation which utilizes zookeeper for logs.
"""
import json
import sys
from logging import Handler, NOTSET, Formatter, LogRecord

from twisted.internet.defer import inlineCallbacks, returnValue

import zookeeper

_error_formatter = Formatter()


class ZookeeperHandler(Handler, object):
    """A logging.Handler implementation that stores records in Zookeeper.

    Intended use is a lightweight, low-volume distributed logging mechanism.
    Records are stored as json strings in sequence nodes.
    """

    def __init__(self, client, context_name, level=NOTSET, log_path="/logs"):
        """Initialize a Zookeeper Log Handler.

        :param client: A connected zookeeper client. The client is managed
                   independently. If the client is shutdown, the handler will
                   no longer emit messages.
        :param context_name: An additional string value denoting the log record
                   context. value will be injected into all emitted records.
        :param level: As per the logging.Handler api, denotes a minimum level
                   that log records must exceed to be emitted from this Handler
        :param log_path: The location within zookeeper of the log records.
        """
        self._client = client
        self._context_name = context_name
        self._log_container_path, self._log_path = self._format_log_path(
            log_path)

        super(ZookeeperHandler, self).__init__(level)

    @property
    def log_container_path(self):
        return self._log_container_path

    @property
    def log_path(self):
        return self._log_path

    def emit(self, record):
        """Emit a log record to zookeeper, enriched with context.

        This method returns a deferred, which the default logging
        implementation will ignore.
        """
        if not self._client.connected:
            return

        json_record = self._format_json(record)
        return self._client.create(
            self._log_path,
            json_record,
            flags=zookeeper.SEQUENCE).addErrback(self._on_error)

    @inlineCallbacks
    def open(self):
        """Ensure the zookeeper logging location exists.

        This extends the :class logging.Handler: api to include an explicit
        open method. In the standard lib Handler implementation the handlers
        typically open their associated resources in __init__ but due
        to the asynchronous nature of zookeeper interaction, this usage
        is not appropriate.
        """
        try:
            yield self._client.create(self._log_container_path)
        except zookeeper.NodeExistsException:
            pass

    def _on_error(self, failure):
        failure.printTraceback(sys.stderr)

    def _format_json(self, record):
        """Format a record into a serialized json dictionary
        """
        data = dict(record.__dict__)
        data["context"] = self._context_name

        if record.exc_info:
            data["message"] = (
                record.getMessage() + "\n" + _error_formatter.formatException(
                    record.exc_info))
            data["exc_info"] = None
        else:
            data["message"] = record.getMessage()
        data["msg"] = data["message"]
        data["args"] = ()
        return json.dumps(data)

    def _format_log_path(self, log_path):
        """Determine the log container path, and log path for records.
        """
        parts = filter(None, log_path.split("/"))
        if len(parts) == 1:
            container_path = "/" + "/".join(parts)
            return (container_path, container_path + "/log-")
        elif len(parts) == 2:
            container_path = "/" + "/".join(parts[:-1])
            return (container_path, container_path + "/" + parts[-1])
        else:
            raise ValueError("invalid log path %r" % log_path)


class LogIterator(object):
    """An iterator over zookeeper stored log entries.

    Provides for reading log entries stored in zookeeper, with a persistent
    position marker, that is updated after size block reads.
    """
    def __init__(
        self, client, replay=False, log_container="/logs", seen_block_size=10):

        self._client = client
        self._container_path = log_container
        self._container_exists = None
        self._seen_block_size = seen_block_size
        self._replay = replay
        self._log_index = None
        self._last_seen_index = 0

    @inlineCallbacks
    def next(self):
        if self._container_exists is None:
            self._container_exists = yield self._wait_for_container()

        if self._log_index is None:
            self._last_seen_index = yield self._get_last_seen()
            if not self._replay:
                self._log_index = self._last_seen_index
            else:
                self._log_index = 0

        log_entry_path = "/logs/log-%010d" % self._log_index

        try:
            data, stat = yield self._client.get(log_entry_path)
        except zookeeper.NoNodeException:
            exists_d, watch_d = self._client.exists_and_watch(
                log_entry_path)
            exists = yield exists_d
            if not exists:
                yield watch_d
            entry = yield self.next()
            returnValue(entry)
        else:
            self._log_index += 1
            if self._replay and self._log_index > self._last_seen_index:
                self._replay = False

            if self._log_index % self._seen_block_size == 0:
                yield self._update_last_seen()
            returnValue(json.loads(data))

    @inlineCallbacks
    def _wait_for_container(self):
        exists_d, watch_d = self._client.exists_and_watch(self._container_path)
        exists = yield exists_d
        if not exists:
            yield watch_d
        returnValue(True)

    def _update_last_seen(self):
        if self._replay:
            return
        data = {"next-log-index": self._log_index}
        return self._client.set(self._container_path, json.dumps(data))

    @inlineCallbacks
    def _get_last_seen(self):
        content, stat = yield self._client.get(self._container_path)
        log_index = 0

        if content:
            data = json.loads(content)
            if isinstance(data, dict):
                log_index = data.get("next-log-index", 0)

        returnValue(log_index)
