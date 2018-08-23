"""The API for publishing messages and consuming from message queues."""

import crochet

from . import exceptions, config
from .signals import pre_publish_signal, publish_signal, publish_failed_signal
from .message import Message
from .twisted.service import FedoraMessagingService


__all__ = (
    "Message",
    "consume",
    "publish",
    "pre_publish_signal",
    "publish_signal",
    "publish_failed_signal",
)


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
    """
    crochet.setup()
    if isinstance(bindings, dict):
        bindings = [bindings]

    service = FedoraMessagingService(callback, bindings=bindings)

    @crochet.run_in_reactor
    def _sub_wrapper(service):
        service.startService()
        return service.getFactory()._halting

    try:
        result = _sub_wrapper(service)
        result.wait()
    except exceptions.HaltConsumer as e:
        return e.exit_code
    finally:
        _stop_service(service)


def publish(message, exchange=None, timeout=15.0):
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
        timeout (float): The time in seconds to wait before giving up and raising
            a :class:`.exceptions.TimeoutError`.

    Raises:
        fedora_messaging.exceptions.PublishReturned: Raised if the broker rejects the
            message.
        fedora_messaging.exceptions.ConnectionException: Raised if a connection error
            occurred before the publish confirmation arrived.
        fedora_messaging.exceptions.ValidationError: Raised if the message
            fails validation with its JSON schema. This only depends on the
            message you are trying to send, the AMQP server is not involved.
    """
    crochet.setup()
    pre_publish_signal.send(publish, message=message)

    if exchange is None:
        exchange = config.conf["publish_exchange"]

    service = FedoraMessagingService(None)
    _start_service(service)

    @crochet.wait_for(timeout=timeout)
    def _pub_wrapper(message, exchange=None):
        return service.getFactory().publish(message, exchange)

    try:
        _pub_wrapper(message, exchange)
        publish_signal.send(publish, message=message)
    except crochet.TimeoutError as e:
        publish_failed_signal.send(publish, message=message, reason=e)
        raise exceptions.ConnectionException(reason=e)
    except exceptions.PublishException as e:
        publish_failed_signal.send(publish, message=message, reason=e)
        raise
    finally:
        _stop_service(service)


@crochet.run_in_reactor
def _start_service(service):
    service.startService()


@crochet.run_in_reactor
def _stop_service(service):
    service.stopService()
