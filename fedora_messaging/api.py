"""The API for publishing messages and consuming from message queues."""

import threading

from . import _session, exceptions, config
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .message import Message, SEVERITIES  # noqa: F401


__all__ = (
    "Message",
    "consume",
    "publish",
    "pre_publish_signal",
    "publish_signal",
    "publish_failed_signal",
)

# Sessions aren't thread-safe, so each thread gets its own
_session_cache = threading.local()


def consume(callback, bindings=None):
    """
    Start a message consumer that executes the provided callback when messages are
    received.

    This API is blocking and will not return until the process receives a signal
    from the operating system.

    The callback receives a single positional argument, the message:

    >>> from fedora_messaging import api
    >>> def my_callback(message):
    ...     print(message)
    >>> bindings = [{'exchange': 'amq.topic', 'queue': 'demo', 'routing_keys': ['#']}]
    >>> api.consume(my_callback, bindings=bindings)

    For complete documentation on writing consumers, see the :ref:`consumers`
    documentation.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`Message`.
        bindings (dict or list of dict): The bindings to use when consuming. This
            should be the same format as the :ref:`conf-bindings` configuration.

    Raises:
        fedora_messaging.exceptions.HaltConsumer: If the consumer requests that
            it be stopped.
        TypeError: If the consumer provide callback that is not a class that
            implements __call__ and is not a function, or if bindings isn't a
            dictionary or list of dictionaries.
    """
    if isinstance(bindings, dict):
        bindings = [bindings]
    if bindings:
        for b in bindings:
            if not isinstance(b, dict):
                raise TypeError("Expected list of dict bindings, got {}".format(bindings))
    session = _session.ConsumerSession()
    session.consume(callback, bindings)


def publish(message, exchange=None):
    """
    Publish a message to an exchange.

    This is a synchronous call, meaning that when this function returns, an
    acknowledgment has been received from the message broker and you can be
    certain the message was published successfully.

    There are some cases where an error occurs despite your message being
    successfully published. For example, if a network partition occurs after
    the message is received by the broker. Therefore, you may publish duplicate
    messages. For complete details, see the :ref:`publishing` documentation.

    >>> from fedora_messaging import api
    >>> message = api.Message(body={'Hello': 'world'}, topic='Hi')
    >>> api.publish(message)

    If an attempt to publish fails because the broker rejects the message, it
    is not retried. Connection attempts to the broker can be configured using
    the "connection_attempts" and "retry_delay" options in the broker URL. See
    :class:`pika.connection.URLParameters` for details.

    Args:
        message (message.Message): The message to publish.
        exchange (str): The name of the AMQP exchange to publish to; defaults to
            :ref:`conf-publish-exchange`

    Raises:
        fedora_messaging.exceptions.PublishReturned: Raised if the broker rejects the
            message.
        fedora_messaging.exceptions.ConnectionException: Raised if a connection error
            occurred before the publish confirmation arrived.
        fedora_messaging.exceptions.ValidationError: Raised if the message
            fails validation with its JSON schema. This only depends on the
            message you are trying to send, the AMQP server is not involved.
    """
    pre_publish_signal.send(publish, message=message)

    if exchange is None:
        exchange = config.conf["publish_exchange"]

    global _session_cache
    if not hasattr(_session_cache, "session"):
        _session_cache.session = _session.PublisherSession()

    try:
        _session_cache.session.publish(message, exchange=exchange)
        publish_signal.send(publish, message=message)
    except exceptions.PublishException as e:
        publish_failed_signal.send(publish, message=message, reason=e)
        raise
