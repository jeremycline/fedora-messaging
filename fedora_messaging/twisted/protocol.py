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
"""
The core Twisted interface, a protocol represent a specific connection to the
AMQP broker. When automatic reconnecting is required, the
:class:`fedora_messaging.twisted.factory.FedoraMessagingFactory` class should
be used to produce configured instances of :class:`.FedoraMessagingProtocol`.

:class:`.FedoraMessagingProtocol` is based on pika's Twisted Protocol. It
implements message schema validation when publishing and subscribing, as well
as a few convenience methods for declaring multiple objects at once.

For an overview of Twisted clients, see the `Twisted client documentation
<https://twistedmatrix.com/documents/current/core/howto/clients.html#protocol>`_.
"""

from __future__ import absolute_import

from collections import namedtuple
import uuid

import pika
import pkg_resources
from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.internet import defer, error, threads, reactor

from twisted.logger import Logger

from .. import config
from ..message import get_message
from ..exceptions import (
    Nack,
    Drop,
    HaltConsumer,
    ValidationError,
    NoFreeChannels,
    BadDeclaration,
    PublishReturned,
    ConnectionException,
)


_log = Logger(__name__)

_pika_version = pkg_resources.get_distribution("pika").parsed_version
if _pika_version < pkg_resources.parse_version("1.0.0b1"):
    ChannelClosedByClient = pika.exceptions.ChannelClosed
else:
    ChannelClosedByClient = pika.exceptions.ChannelClosedByClient


#: A namedtuple that represents a AMQP consumer.
#:
#: * The ``tag`` field is the consumer's AMQP tag (:class:`str`).
#: * The ``queue`` field is the name of the queue it's consuming from (:class:`str`).
#: * The ``callback`` field is the function called for each message (a callable).
#: * The ``channel`` is the AMQP channel used for the consumer
#:   (:class:`pika.adapters.twisted_connection.TwistedChannel`).
Consumer = namedtuple("Consumer", ["tag", "queue", "callback", "channel", "deferred"])


class ConsumerV2(object):
    """
    Replace the namedtuple with this, we need to mutate running

    Attributes:
        queue (str): The AMQP queue this consumer is subscribed to.
        callback (callable): The callback to run when a message arrives.
        result (defer.Deferred): A deferred that runs the callbacks if the
            consumer exits gracefully by raising HaltConsumer or being canceled
            and errbacks if an unhandled exception occurs in the callback, or if
            HaltConsumer is raised with a non-zero exit code.
    """

    def __init__(self, queue=None, callback=None):
        self.queue = queue
        self.callback = callback
        self.result = None

        # The current channel used by this consumer.
        self._channel = None
        # The unique ID for the AMQP consumer.
        self._tag = str(uuid.uuid4())
        # Used to start and stop the consumer via cancel(), pauseProducing()
        self._running = True


class FedoraMessagingProtocol(TwistedProtocolConnection):
    """A Twisted Protocol for the Fedora Messaging system.

    This protocol builds on the generic pika AMQP protocol to add calls
    specific to the Fedora Messaging implementation.

    Attributes:
        factory: The :class:`Factory` object that created this protocol. This
            is expected to contain the following attributes: confirms (bool).

    Args:
        parameters (pika.ConnectionParameters): The connection parameters.
        confirms (bool): If True, all outgoing messages will require a
            confirmation from the server, and the Deferred returned from
            the publish call will wait for that confirmation.
    """

    name = u"FedoraMessaging:Protocol"

    def __init__(self, parameters, confirms=True):
        TwistedProtocolConnection.__init__(self, parameters)
        if confirms and _pika_version < pkg_resources.parse_version("1.0.0b1"):
            _log.error("Message confirmation is only available with pika 1.0.0+")
            confirms = False
        self._confirms = confirms
        self._channel = None
        # Map queue names to dictionaries representing consumers
        self._consumers = {}

    @defer.inlineCallbacks
    def _allocate_channel(self):
        """
        Allocate a new AMQP channel.

        Raises:
            NoFreeChannels: If this connection has reached its maximum number of channels.
        """
        try:
            channel = yield self.channel()
        except pika.exceptions.NoFreeChannels:
            raise NoFreeChannels()
        _log.info("Created AMQP channel {num}", num=channel.channel_number)
        if self._confirms:
            yield channel.confirm_delivery()
        defer.returnValue(channel)

    @defer.inlineCallbacks
    def connectionReady(self, res=None):
        """
        Callback invoked when the AMQP connection is ready (when self.ready fires).

        This API is not meant for users.

        Args:
            res: This is an unused argument that provides compatibility with Pika
                versions lower than 1.0.0.
        """
        self._channel = yield self._allocate_channel()
        if _pika_version < pkg_resources.parse_version("1.0.0b1"):
            extra_args = dict(all_channels=True)
        else:
            extra_args = dict(global_qos=True)
        yield self._channel.basic_qos(
            prefetch_count=config.conf["qos"]["prefetch_count"],
            prefetch_size=config.conf["qos"]["prefetch_size"],
            **extra_args
        )
        if _pika_version < pkg_resources.parse_version("1.0.0b1"):
            TwistedProtocolConnection.connectionReady(self, res)

    @defer.inlineCallbacks
    def _read(self, queue_object, consumer):
        """
        The loop that reads from the message queue and calls the consumer callback
        wrapper.

        Serialized Processing
        ---------------------
        This loop processes messages serially. This is because a second
        ``queue_object.get()`` operation can only occur after the Deferred from
        ``self._on_message`` completes. Thus, we can be sure that callbacks
        never run concurrently in two different threads.

        This is done rather than saturating the Twisted thread pool as the
        documentation for callbacks (in fedmsg and here) has never indicated
        that they are not thread-safe.


        Gracefully Halting
        ------------------
        This is a loop that only exits when self._running is set to False.
        This occurs in :meth:`pauseProducing`, which sets it to False and waits
        for all Deferred _read calls to complete before cancelling consumers on
        the broker and re-queuing any pre-fetched messages.

        The Deferred object only completes when this method returns, so we need
        to periodically check the status of self._running. That's why there's a
        short timeout on the call to ``queue_object.get``. If self._running is
        set to False, this method will return within the timeout, allowing for
        the reactor to gracefully halt when there are no messages queued and none
        being processed.

        If a message is being processed when self._running is set, this
        method's Deferred won't complete until ``self._on_message`` returns, so
        the connection to the broker remains open until that occurs.  This
        allows us to ack/nack the current message before the reactor shuts
        down.


        queue_object (pika.adapters.twisted_connection.ClosableDeferredQueue):
            The AMQP queue the consumer is bound to.
        consumer (dict): A dictionary describing the consumer for the given
            queue_object.
        """
        while consumer._running:
            try:
                deferred_get = queue_object.get()
                deferred_get.addTimeout(1, reactor)
                channel, delivery_frame, properties, body = yield deferred_get
            except defer.TimeoutError:
                continue

            _log.debug(
                "Message arrived with delivery tag {tag} for {consumer}",
                tag=delivery_frame.delivery_tag,
                consumer=consumer._tag,
            )
            try:
                message = get_message(delivery_frame.routing_key, properties, body)
                message.queue = consumer.queue
            except ValidationError:
                _log.warn(
                    "Message id {msgid} did not pass validation; ignoring message",
                    msgid=properties.message_id,
                )
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=False
                )
                continue

            try:
                _log.info(
                    "Consuming message from topic {topic!r} (id {msgid})",
                    topic=message.topic,
                    msgid=properties.message_id,
                )
                yield threads.deferToThread(consumer.callback, message)
            except Nack:
                _log.warn(
                    "Returning message id {msgid} to the queue", msgid=properties.message_id
                )
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=True
                )
            except Drop:
                _log.warn(
                    "Consumer requested message id {msgid} be dropped",
                    msgid=properties.message_id,
                )
                yield channel.basic_nack(
                    delivery_tag=delivery_frame.delivery_tag, requeue=False
                )
            except HaltConsumer as e:
                _log.info("Consumer indicated it wishes consumption to halt, shutting down")
                if e.requeue:
                    yield channel.basic_nack(
                        delivery_tag=delivery_frame.delivery_tag, requeue=True
                    )
                else:
                    yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
                if e.exit_code == 0:
                    break
                else:
                    raise
            except Exception:
                _log.failure("Received unexpected exception from consumer {c}", c=consumer)
                yield channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
                raise
            else:
                _log.info(
                    "Successfully consumed message from topic {topic!r} (id {msgid})",
                    topic=message.topic,
                    msgid=properties.message_id,
                )
                yield channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)

    @defer.inlineCallbacks
    def publish(self, message, exchange):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker.

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            PublishReturned: If the broker rejected the message. This can happen if
                there are resource limits that have been reached (full disk, for example)
                or if the message will be routed to 0 queues and the exchange is set to
                reject such messages.

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        message.validate()
        try:
            yield self._channel.basic_publish(
                exchange=exchange,
                routing_key=message._encoded_routing_key,
                body=message._encoded_body,
                properties=message._properties,
            )
        except (pika.exceptions.NackError, pika.exceptions.UnroutableError) as e:
            _log.error("Message was rejected by the broker ({reason})", reason=str(e))
            raise PublishReturned(reason=e)
        except pika.exceptions.ChannelClosed:
            self._channel = yield self._allocate_channel()
            yield self.publish(message, exchange)
        except pika.exceptions.ConnectionClosed as e:
            raise ConnectionException(reason=e)

    @defer.inlineCallbacks
    def consume(self, callback, queue):
        """
        Register a message consumer that executes the provided callback when
        messages are received.

        The queue must exist prior to calling this method.  If a consumer
        already exists for the given queue, the callback is simply updated and
        any new messages for that consumer use the new callback.

        If :meth:`resumeProducing` has not been called when this method is called,
        it will be called for you.

        Args:
            callback (callable): The callback to invoke when a message is received.
            queue (str): The name of the queue to consume from.

        Returns:
            Deferred: A Deferred that fires when the consumer halts or crashes.
                The caller can also use this to halt the consumer by calling cancel
                on the returned Deferred.
            Consumer: A namedtuple that identifies this consumer.

        NoFreeChannels: If there are no available channels on this connection.
            If this occurs, you can either reduce the number of consumers on this
            connection or create an additional connection.
        """
        if queue in self._consumers:
            self._consumers[queue].callback = callback
            return

        consumer = ConsumerV2(queue=queue, callback=callback)
        consumer._channel = yield self._allocate_channel()
        queue_object, _ = yield consumer._channel.basic_consume(
            queue=consumer.queue, consumer_tag=consumer._tag
        )
        consumer.result = self._read(queue_object, consumer)
        self._consumers[queue] = consumer
        _log.info("Successfully registered AMQP consumer {c}", c=consumer)
        defer.returnValue(consumer)

    @defer.inlineCallbacks
    def cancel(self, consumer):
        consumer._running = False

        # Wait for the last message to finish.
        yield consumer.result

        del self._consumers[consumer.queue]

        try:
            yield consumer._channel.basic_cancel(consumer_tag=consumer._tag)
        except pika.exceptions.AMQPChannelError:
            # Consumers are tied to channels, so if this channel is dead the
            # consumer should already be canceled (and we can't get to it anyway)
            pass
        try:
            yield consumer._channel.close()
        except pika.exceptions.AMQPChannelError:
            pass

    @defer.inlineCallbacks
    def declare_exchanges(self, exchanges):
        """
        Declare a number of exchanges at once.

        This simply wraps the :meth:`pika.channel.Channel.exchange_declare`
        method and deals with error handling and channel allocation.

        Args:
            exchanges (list of dict): A list of dictionaries, where each dictionary
                represents an exchange. Each dictionary can have the following keys:

                  * exchange (str): The exchange's name
                  * exchange_type (str): The type of the exchange ("direct", "topic", etc)
                  * passive (bool): If true, this will just assert that the exchange exists,
                    but won't create it if it doesn't.
                  * durable (bool): Whether or not the exchange is durable
                  * arguments (dict): Extra arguments for the exchange's creation.
        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If an exchange could not be declared. This can occur
                if the exchange already exists, but does its type does not match
                (e.g. it is declared as a "topic" exchange, but exists as a "direct"
                exchange). It can also occur if it does not exist, but the current
                user does not have permissions to create the object.
        """
        channel = yield self._allocate_channel()
        try:
            for exchange in exchanges:
                try:
                    yield channel.exchange_declare(**exchange)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("exchange", exchange, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def declare_queues(self, queues):
        """
        Declare a list of queues.

        Args:
            queues (list of dict): A list of dictionaries, where each dictionary
                represents an exchange. Each dictionary can have the following keys:

                  * queue (str): The name of the queue
                  * passive (bool): If true, this will just assert that the queue exists,
                    but won't create it if it doesn't.
                  * durable (bool): Whether or not the queue is durable
                  * exclusive (bool): Whether or not the queue is exclusive to this connection.
                  * auto_delete (bool): Whether or not the queue should be automatically
                    deleted once this connection ends.
                  * arguments (dict): Additional arguments for the creation of the queue.
        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If a queue could not be declared. This can occur
                if the queue already exists, but does its type does not match
                (e.g. it is declared as a durable queue, but exists as a non-durable
                queue). It can also occur if it does not exist, but the current
                user does not have permissions to create the object.
        """
        channel = yield self._allocate_channel()
        try:
            for queue in queues:
                try:
                    yield channel.queue_declare(**queue)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("queue", queue, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def bind_queues(self, bindings):
        """
        Declare a set of bindings between queues and exchanges.

        Args:
            bindings (list of dict): A list of binding definitions. Each dictionary
                must contain the "queue" key whose value is the name of the queue
                to create the binding on, as well as the "exchange" key whose value
                should be the name of the exchange to bind to. Additional acceptable
                keys are any keyword arguments accepted by
                :meth:`pika.channel.Channel.queue_bind`.

        Raises:
            NoFreeChannels: If there are no available channels on this connection.
                If this occurs, you can either reduce the number of consumers on this
                connection or create an additional connection.
            BadDeclaration: If a binding could not be declared. This can occur if the
                queue or exchange don't exist, or if they do, but the current user does
                not have permissions to create bindings.
        """
        channel = yield self._allocate_channel()
        try:
            for binding in bindings:
                try:
                    yield channel.queue_bind(**binding)
                except pika.exceptions.ChannelClosed as e:
                    raise BadDeclaration("binding", binding, e)
        finally:
            try:
                channel.close()
            except pika.exceptions.AMQPError:
                pass  # pika doesn't handle repeated closes gracefully

    @defer.inlineCallbacks
    def resumeProducing(self):
        """
        Starts or resumes the retrieval of messages from the server queue.

        This method starts receiving messages from the server, they will be
        passed to the consumer callback.

        .. note:: This is called automatically when :meth:`.consume` is called,
            so users should not need to call this unless :meth:`.pauseProducing`
            has been called.

        Returns:
            defer.Deferred: fired when the production is ready to start
        """
        for consumer in self._consumers.values():
            consumer._running = True
            queue_object, _ = yield consumer._channel.basic_consume(
                queue=consumer.queue, consumer_tag=consumer._tag
            )
            consumer.result = self._read(queue_object, consumer)
            consumer.result.addErrback(
                lambda f: _log.failure, "_read failed on consumer {c}", c=consumer
            )
        _log.info("All AMQP consumers have successfully been started.")

    @defer.inlineCallbacks
    def pauseProducing(self):
        """
        Pause the reception of messages by canceling all existing consumers.
        This does not disconnect from the server.

        Message reception can be resumed with :meth:`resumeProducing`.

        Returns:
            Deferred: fired when the production is paused.
        """
        for c in self._consumers.values():
            _log.info("{c!r}", c=c)
            c._running = False
        read_loops = [c.result for c in self._consumers.values()]
        _log.info(
            "Waiting for {n} consumer(s) to finish processing before canceling the consumer",
            n=len(read_loops)
        )
        yield defer.gatherResults(read_loops)

        for consumer in self._consumers.values():
            yield consumer._channel.basic_cancel(consumer_tag=consumer._tag)
            try:
                yield consumer._channel.close()
            except pika.exceptions.AMQPChannelError:
                pass
        _log.info("{n} consumers canceled", n=len(self._consumers))

    @defer.inlineCallbacks
    def stopProducing(self):
        """
        Stop producing messages and disconnect from the server.

        Returns:
            Deferred: fired when the production is stopped.
        """
        _log.info("Starting to disconnect from the AMQP broker")
        if self.is_closed:
            # We were asked to stop because the connection is already gone.
            # There's no graceful way to stop because we can't acknowledge
            # messages in the middle of being processed.

            self._channel = None
            return
        yield self.pauseProducing()
        yield self.close()
        self._consumers = {}
        self._channel = None
