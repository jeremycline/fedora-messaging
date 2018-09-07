# This file is part of fedora_messaging.
# Copyright (C) 2018 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along
# with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
from __future__ import absolute_import, unicode_literals

import inspect
import logging
import signal
import ssl

from pika import exceptions as pika_errs
import pika
import pkg_resources

try:
    # Versions of pika greater than the 0.11 series uses SSLOptions to configure
    # a TLS connection.
    from pika import SSLOptions
except ImportError:
    SSLOptions = None

from . import config
from .message import get_message
from .exceptions import (
    Nack,
    Drop,
    HaltConsumer,
    ValidationError,
    PublishReturned,
    ConnectionException,
    ConfigurationException,
)

_log = logging.getLogger(__name__)

# pika 0.12 introduces the SSLOptions object in 0.12, but it doesn't have the
# same API that 1.0.0 has. Additionally, the connection parameters still expect
# the old dictionary, so mark SSLOptions as None if this is 0.12
if pkg_resources.get_distribution("pika").version.startswith("0.12."):
    SSLOptions = None


def _configure_tls_parameters(parameters):
    """
    Configure the pika connection parameters for TLS based on the configuration.

    This modifies the object provided to it. This accounts for whether or not
    the new API based on the standard library's SSLContext is available for
    pika.

    Args:
        parameters (pika.ConnectionParameters): The connection parameters to apply
            TLS connection settings to.
    """
    cert = config.conf["tls"]["certfile"]
    key = config.conf["tls"]["keyfile"]
    if cert and key:
        _log.info(
            "Authenticating with server using x509 (certfile: %s, keyfile: %s)",
            cert,
            key,
        )
        parameters.credentials = pika.credentials.ExternalCredentials()
    else:
        cert, key = None, None

    if SSLOptions is None:
        parameters.ssl = True
        parameters.ssl_options = {
            "keyfile": key,
            "certfile": cert,
            "ca_certs": config.conf["tls"]["ca_cert"],
            "cert_reqs": ssl.CERT_REQUIRED,
            "ssl_version": ssl.PROTOCOL_TLSv1_2,
        }
    else:
        ssl_context = ssl.create_default_context()
        try:
            ssl_context.load_verify_locations(cafile=config.conf["tls"]["ca_cert"])
        except ssl.SSLError as e:
            raise ConfigurationException(
                'The "ca_cert" setting in the "tls" section is invalid ({})'.format(e)
            )
        ssl_context.options |= ssl.OP_NO_SSLv2
        ssl_context.options |= ssl.OP_NO_SSLv3
        ssl_context.options |= ssl.OP_NO_TLSv1
        ssl_context.options |= ssl.OP_NO_TLSv1_1
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.check_hostname = True
        if cert and key:
            try:
                ssl_context.load_cert_chain(cert, key)
            except ssl.SSLError as e:
                raise ConfigurationException(
                    'The "keyfile" setting in the "tls" section is invalid ({})'.format(
                        e
                    )
                )
        parameters.ssl_options = SSLOptions(
            ssl_context, server_hostname=parameters.host
        )


class PublisherSession(object):
    """A session with blocking APIs for publishing to an AMQP broker."""

    def __init__(self, amqp_url=None, confirms=True):
        amqp_url = amqp_url or config.conf["amqp_url"]
        self._parameters = pika.URLParameters(amqp_url)
        if amqp_url.startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]
        self._confirms = confirms
        self._connection = None
        self._channel = None

    def publish(self, message, exchange=None):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_ on
        the message broker.

        >>> from fedora_messaging import _session, message
        >>> msg = message.Message(topic='test', body={'test':'message'})
        >>> sess = session.BlockingSession()
        >>> sess.publish(msg)

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to; defaults
                to :ref:`conf-publish-exchange`

        Raises:
            PublishReturned: If the published message is rejected by the broker.
            ConnectionException: If a connection error occurs while publishing.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        message.validate()
        try:
            self._connect_and_publish(exchange, message)
        except (pika_errs.ConnectionClosed, pika_errs.ChannelClosed) as e:
            # Because this is a blocking connection (and thus can't heartbeat)
            # we might need to restart the connection.
            _log.info("Resetting connection to %s", self._parameters.host)
            self._connection = self._channel = None
            try:
                self._connect_and_publish(exchange, message)
            except (pika_errs.NackError, pika_errs.UnroutableError) as e:
                _log.warning("Message was rejected by the broker (%s)", str(e))
                raise PublishReturned(reason=e)
            except pika_errs.AMQPError as e:
                _log.error(str(e))
                if self._connection and self._connection.is_open:
                    self._connection.close()
                raise ConnectionException(reason=e)
        except (pika_errs.NackError, pika_errs.UnroutableError) as e:
            _log.warning("Message was rejected by the broker (%s)", str(e))
            raise PublishReturned(reason=e)
        except pika_errs.AMQPError as e:
            if self._connection and self._connection.is_open:
                self._connection.close()
            raise ConnectionException(reason=e)

    def _connect_and_publish(self, exchange, message):
        if not self._connection or not self._channel:
            self._connection = pika.BlockingConnection(self._parameters)
            self._channel = self._connection.channel()
            if self._confirms:
                self._channel.confirm_delivery()

        self._channel.publish(
            exchange=exchange,
            routing_key=message._encoded_routing_key,
            body=message._encoded_body,
            properties=message._properties,
        )


class ConsumerSession(object):
    """A session using the asynchronous APIs offered by Pika."""

    def __init__(self):
        self._parameters = pika.URLParameters(config.conf["amqp_url"])
        if config.conf["amqp_url"].startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]
        self._connection = None
        self._channel = None
        self._bindings = []
        self._running = False

        def _signal_handler(signum, frame):
            """
            Signal handler that gracefully shuts down the consumer

            Args:
                signum (int): The signal this process received.
                frame (frame): The current stack frame (unused).
            """
            if signum in (signal.SIGTERM, signal.SIGINT):
                self._shutdown()

        signal.signal(signal.SIGTERM, _signal_handler)
        signal.signal(signal.SIGINT, _signal_handler)

    def _shutdown(self):
        """Gracefully shut down the consumer and exit."""
        if self._channel:
            _log.info("Halting %r consumer sessions", self._channel.consumer_tags)
        self._running = False
        if self._connection and self._connection.is_open:
            self._connection.close()
        # Reset the signal handler
        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, signal.SIG_DFL)

    def _on_cancelok(self, cancel_frame):
        """
        Called when the server acknowledges a cancel request.

        Args:
            cancel_frame (pika.spec.Basic.CancelOk): The cancelok frame from
                the server.
        """
        _log.info("Consumer canceled; returning all unprocessed messages to the queue")
        self._channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)

    def _on_channel_open(self, channel):
        """
        Callback used when a channel is opened.

        This registers all the channel callbacks.

        Args:
            channel (pika.channel.Channel): The channel that successfully opened.
        """
        channel.add_on_close_callback(self._on_channel_close)
        channel.add_on_cancel_callback(self._on_cancel)

        channel.basic_qos(callback=self._on_qosok, **config.conf["qos"])

    def _on_qosok(self, qosok_frame):
        """
        Callback invoked when the server acknowledges the QoS settings.

        Asserts or creates the exchanges and queues exist.

        Args:
            qosok_frame (pika.spec.Basic.Qos): The frame send from the server.
        """
        for name, args in self._exchanges.items():
            self._channel.exchange_declare(
                exchange=name,
                exchange_type=args["type"],
                durable=args["durable"],
                auto_delete=args["auto_delete"],
                arguments=args["arguments"],
                callback=self._on_exchange_declareok,
            )
        for name, args in self._queues.items():
            self._channel.queue_declare(
                queue=name,
                durable=args["durable"],
                auto_delete=args["auto_delete"],
                exclusive=args["exclusive"],
                arguments=args["arguments"],
                callback=self._on_queue_declareok,
            )

    def _on_channel_close(self, channel, reply_code_or_reason, reply_text=None):
        """
        Callback invoked when the channel is closed.

        Args:
            channel (pika.channel.Channel): The channel that got closed.
            reply_code_or_reason (int|Exception): The reason why the channel
                was closed. In older versions of pika, this is the AMQP code.
            reply_text (str): The human-readable reason for the channel's
                closure (only in older versions of pika).
        """
        if isinstance(reply_code_or_reason, pika_errs.ChannelClosed):
            reply_code = reply_code_or_reason.reply_code
            reply_text = reply_code_or_reason.reply_text
        elif isinstance(reply_code_or_reason, int):
            reply_code = reply_code_or_reason
        else:
            reply_code = 0
            reply_text = str(reply_code_or_reason)

        _log.info("Channel %r closed (%d): %s", channel, reply_code, reply_text)
        self._channel = None

    def _on_connection_open(self, connection):
        """
        Callback invoked when the connection is successfully established.

        Args:
            connection (pika.connection.SelectConnection): The newly-estabilished
                connection.
        """
        _log.info("Successfully opened connection to %s", connection.params.host)
        self._channel = connection.channel(on_open_callback=self._on_channel_open)

    def _on_connection_close(self, connection, reply_code_or_reason, reply_text=None):
        """
        Callback invoked when a previously-opened connection is closed.

        Args:
            connection (pika.connection.SelectConnection): The connection that
                was just closed.
            reply_code_or_reason (int|Exception): The reason why the channel
                was closed. In older versions of pika, this is the AMQP code.
            reply_text (str): The human-readable reason the connection was
                closed (only in older versions of pika)
        """
        self._channel = None

        if isinstance(reply_code_or_reason, pika_errs.ConnectionClosed):
            reply_code = reply_code_or_reason.reply_code
            reply_text = reply_code_or_reason.reply_text
        elif isinstance(reply_code_or_reason, int):
            reply_code = reply_code_or_reason
        else:
            reply_code = 0
            reply_text = str(reply_code_or_reason)

        if reply_code == 200:
            # Normal shutdown, exit the consumer.
            _log.info("Server connection closed (%s), shutting down", reply_text)
            connection.ioloop.stop()
        else:
            _log.warning(
                "Connection to %s closed unexpectedly (%d): %s",
                connection.params.host,
                reply_code,
                reply_text,
            )
            self.call_later(1, self.reconnect)  # TODO: exponential backoff?

    def _on_connection_error(self, connection, error_message):
        """
        Callback invoked when the connection failed to be established.

        Args:
            connection (pika.connection.SelectConnection): The connection that
                failed to open.
            error_message (str): The reason the connection couldn't be opened.
        """
        self._channel = None
        if isinstance(error_message, pika_errs.AMQPConnectionError):
            error_message = repr(error_message.args[0])
        _log.error(error_message)
        self.call_later(1, self.reconnect)  # TODO: exponential backoff?

    def _on_exchange_declareok(self, declare_frame):
        """
        Callback invoked when an exchange is successfully declared.

        It will declare the queues in the bindings dictionary with the
        :meth:`_on_queue_declareok` callback.

        Args:
            frame (pika.spec.Exchange.DeclareOk): The DeclareOk frame from the
                server.
        """
        _log.info("Exchange declared successfully")

    def _on_queue_declareok(self, frame):
        """
        Callback invoked when a queue is successfully declared.

        Args:
            frame (pika.frame.Method): The message sent from the server.
        """
        _log.info("Successfully declared the %s queue", frame.method.queue)
        for binding in self._bindings:
            if binding["queue"] == frame.method.queue:
                for key in binding["routing_keys"]:
                    _log.info(
                        "Asserting %s is bound to %s with the %s key",
                        binding["queue"],
                        binding["exchange"],
                        key,
                    )
                    self._channel.queue_bind(
                        callback=None,
                        queue=binding["queue"],
                        exchange=binding["exchange"],
                        routing_key=key,
                    )
                bc_args = dict(queue=frame.method.queue)
                if pkg_resources.get_distribution("pika").version.startswith("0.12."):
                    bc_args["consumer_callback"] = self._on_message
                else:
                    bc_args["on_message_callback"] = self._on_message
                self._channel.basic_consume(**bc_args)

    def _on_cancel(self, cancel_frame):
        """
        Callback used when the server sends a consumer cancel frame.

        Args:
            cancel_frame (pika.spec.Basic.Cancel): The cancel frame from
                the server.
        """
        _log.info("Server canceled consumer")

    def call_later(self, delay, callback):
        """Schedule a one-shot timeout given delay seconds.

        This method is only useful for compatibility with older versions of pika.

        Args:
            delay (float): Non-negative number of seconds from now until
                expiration
            callback (method): The callback method, having the signature
                `callback()`
        """
        if hasattr(self._connection.ioloop, "call_later"):
            self._connection.ioloop.call_later(delay, callback)
        else:
            self._connection.ioloop.add_timeout(delay, callback)

    def connect(self):
        _log.info("Connecting to %s:%d", self._parameters.host, self._parameters.port)
        self._connection = pika.SelectConnection(
            self._parameters,
            on_open_callback=self._on_connection_open,
            on_open_error_callback=self._on_connection_error,
            on_close_callback=self._on_connection_close,
        )

    def reconnect(self):
        """Will be invoked by the IOLoop timer if the connection is
        closed. See the _on_connection_close method.
        """
        # This is the old connection instance, stop its ioloop.
        self._connection.ioloop.stop()
        if self._running:
            # Create a new connection
            self.connect()
            # There is now a new connection, needs the new ioloop to run.
            self._connection.ioloop.start()

    def consume(self, callback, bindings=None, queues=None, exchanges=None):
        """
        Consume messages from a message queue.

        Simply define a callable to be used as the callback when messages are
        delivered and specify the queue bindings. This call blocks. The callback
        signature should accept a single positional argument which is an
        instance of a :class:`Message` (or a sub-class of it).

        Args:
            callback (callable): The callable to pass the message to when one
                arrives.
            bindings (list of dict): A list of dictionaries describing bindings
                for queues. Refer to the :ref:`conf-bindings` configuration
                documentation for the format.
            queues (dict): A dictionary of queues to ensure exist. Refer to the
                :ref:`conf-queues` configuration documentation for the format.
            exchanges (dict): A dictionary of exchanges to ensure exist. Refer
                to the :ref:`conf-exchanges` configuration documentation for the
                format.

        Raises:
            HaltConsumer: Raised when the consumer halts.
            TypeError: If the callback isn't a function or a class with __call__
                defined.
        """
        self._bindings = bindings or config.conf["bindings"]
        self._queues = queues or config.conf["queues"]
        self._exchanges = exchanges or config.conf["exchanges"]

        # If the callback is a class, create an instance of it first
        if inspect.isclass(callback):
            self._consumer_callback = callback()
        elif inspect.isfunction(callback):
            self._consumer_callback = callback
        else:
            raise TypeError(
                "Callback must be a class that implements __call__" " or a function."
            )
        self._running = True
        self.connect()
        self._connection.ioloop.start()

    def _on_message(self, channel, delivery_frame, properties, body):
        """
        Callback when a message is received from the server.

        This method wraps a user-registered callback for message delivery. It
        decodes the message body, determines the message schema to validate the
        message with, and validates the message before passing it on to the user
        callback.

        This also handles acking, nacking, and rejecting messages based on
        exceptions raised by the consumer callback. For detailed documentation
        on the user-provided callback, see the user guide on consuming.

        Args:
            channel (pika.channel.Channel): The channel from which the message
                was received.
            delivery_frame (pika.spec.Deliver): The delivery frame which includes
                details about the message like content encoding and its delivery
                tag.
            properties (pika.spec.BasicProperties): The message properties like
                the message headers.
            body (bytes): The message payload.

        Raises:
            HaltConsumer: Raised when the consumer halts.
        """
        _log.debug("Message arrived with delivery tag %s", delivery_frame.delivery_tag)

        try:
            message = get_message(delivery_frame.routing_key, properties, body)
        except ValidationError:
            channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=False)
            return

        try:
            _log.info(
                'Consuming message from topic "%s" (id %s)',
                message.topic,
                properties.message_id,
            )
            self._consumer_callback(message)
            channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
        except Nack:
            _log.info("Returning message id %s to the queue", properties.message_id)
            channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=True)
        except Drop:
            _log.info("Dropping message id %s", properties.message_id)
            channel.basic_nack(delivery_tag=delivery_frame.delivery_tag, requeue=False)
        except HaltConsumer as e:
            _log.info(
                "Consumer requested halt on message id %s with requeue=%s",
                properties.message_id,
                e.requeue,
            )
            channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=e.requeue
            )
            self._shutdown()
            if e.exit_code != 0:
                raise
        except Exception as e:
            _log.exception("Received unexpected exception from consumer callback")
            channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
            self._shutdown()
            raise HaltConsumer(exit_code=1, reason=e, requeue=True)
