"""The API for publishing messages and consuming from message queues."""

import threading

from twisted.internet import ssl, defer, protocol, error, threads, reactor

from . import _session, exceptions, config
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .message import Message, SEVERITIES  # noqa: F401
from .twisted import service


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
_twisted_service = None


def twisted_consume(callback, bindings=None, queues=None):
    """
    Start a consumer using the provided callback and run it using the Twisted
    event loop (reactor).

    .. warning:: Callbacks run in a Twisted-managed thread pool to avoid them
        blocking the event loop. If you wish to use Twisted APIs in your callback
        you must use the blockingCallFromThread or callFromThread APIs. Note,
        however that messages are processed serially so there is not speed
        advantage to this presently.

    This API expects the caller to start the reactor.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`Message` or a class object that implements the ``__call__``
            method. The class will be instantiated before use.
        bindings (dict or list of dict): Bindings to declare before consuming. This
            should be the same format as the :ref:`conf-bindings` configuration.
        queue (dict): The queue to declare and consume from. This dictionary should
            have the "queue", "durable", "auto_delete", "exclusive", and "arguments"
            keys.

    Returns:
        List of Deferred:
            A list of deferred objects that fire with a representation of the
            consumer when it is successfully registered and running. Note that
            this API is meant to survive network problems, so consuming will
            continue until a corresponding call to :func:`twisted_consume_cancel`
            is called.
    """
    global _twisted_service
    if _twisted_service is None:
        _twisted_service = service.FedoraMessagingService(config.conf["amqp_url"])
        reactor.callWhenRunning(_twisted_service.startService)
        # Twisted is killing the underlying connection before stopService gets
        # called, so we need to add it as a pre-shutdown event to gracefully
        # finish up messages in progress.
        reactor.addSystemEventTrigger("before", "shutdown", _twisted_service.stopService)

    return _twisted_service.factory.consume(callback, bindings, queues)


def twisted_consume_cancel(consumer):
    """
    Cancel a consumer that was previously started from :func:`.twisted_consume`.

    Consumers that are canceled are allowed to finish processing any messages before
    halting.

    Args:
        consumer (dict or Deferred): The consumer to cancel. These consumer
            objects are returned by the call to :func:`.twsted_consume`.

    Returns:
        Deferred: A Deferred that fires when the consumer is successfully canceled.
    """
    global _twisted_service
    if _twisted_service is None:
        raise ValueError("There is no Twisted service running")

    return _twisted_service.cancel(consumer)


def consume(callback, bindings=None, queues=None):
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
    >>> queues = {
    ...     "demo": {"durable": False, "auto_delete": True, "exclusive": True, "arguments": {}}
    ... }
    >>> api.consume(my_callback, bindings=bindings, queues=queues)

    If the bindings and queue arguments are not provided, they will be loaded from
    the configuration.

    For complete documentation on writing consumers, see the :ref:`consumers`
    documentation.

    Args:
        callback (callable): A callable object that accepts one positional argument,
            a :class:`Message` or a class object that implements the ``__call__``
            method. The class will be instantiated before use.
        bindings (dict or list of dict): Bindings to declare before consuming. This
            should be the same format as the :ref:`conf-bindings` configuration.
        queues (dict): The queue or queues to declare and consume from. This should be
            in the same format as the :ref:`conf-queues` configuration dictionary where
            each key is a queue name and each value is a dictionary of settings for that
            queue.

    Raises:
        fedora_messaging.exceptions.HaltConsumer: If the consumer requests that
            it be stopped.
        ValueError: If the consumer provide callback that is not a class that
            implements __call__ and is not a function, if the bindings argument
            is not a dict or list of dicts with the proper keys, or if the queues
            argument isn't a dict with the proper keys.
    """
    if isinstance(bindings, dict):
        bindings = [bindings]

    if bindings is None:
        bindings = config.conf["bindings"]
    else:
        try:
            config.validate_bindings(bindings)
        except exceptions.ConfigurationException as e:
            raise ValueError(e.message)

    if queues is None:
        queues = config.conf["queues"]
    else:
        try:
            config.validate_queues(queues)
        except exceptions.ConfigurationException as e:
            raise ValueError(e.message)

    session = _session.ConsumerSession()
    session.consume(callback, bindings=bindings, queues=queues)


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
