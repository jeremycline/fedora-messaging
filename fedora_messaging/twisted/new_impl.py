
# Use the existing protocol, mangled to change the consume API as necessary. Try the endpoints API?
# present since 10.1, but probably not worth it at the moment


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
Twisted Service to start and stop the Fedora Messaging Twisted Factory.

This Service makes it easier to build a Twisted application that embeds a
Fedora Messaging component. See the ``verify_missing`` service in
fedmsg-migration-tools for a use case.

See https://twistedmatrix.com/documents/current/core/howto/application.html
"""

from __future__ import absolute_import, unicode_literals
from collections import namedtuple
import locale
import pkg_resources
import uuid

from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.application import service
from twisted.application.internet import TCPClient, SSLClient
from twisted.internet import ssl, defer, protocol, error, threads, reactor
from twisted.logger import Logger
import pika
import six

from .. import config
from .._session import _configure_tls_parameters
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


def _ssl_context_factory(parameters):
    """
    Produce a Twisted SSL context object from a pika connection parameter object.
    This is necessary as Twisted manages the connection, not Pika.

    Args:
        parameters (pika.ConnectionParameters): The connection parameters built
            from the fedora_messaging configuration.
    """
    client_cert = None
    ca_cert = None
    key = config.conf["tls"]["keyfile"]
    cert = config.conf["tls"]["certfile"]
    ca_file = config.conf["tls"]["ca_cert"]
    if ca_file:
        with open(ca_file, "rb") as fd:
            # Open it in binary mode since otherwise Twisted will immediately
            # re-encode it as ASCII, which won't work if the cert bundle has
            # comments that can't be encoded with ASCII.
            ca_cert = ssl.Certificate.loadPEM(fd.read())
    if key and cert:
        # Note that _configure_tls_parameters sets the auth mode to EXTERNAL
        # if both key and cert are defined, so we don't need to do that here.
        with open(key) as fd:
            client_keypair = fd.read()
        with open(cert) as fd:
            client_keypair += fd.read()
        client_cert = ssl.PrivateCertificate.loadPEM(client_keypair)

    hostname = parameters.host
    if not isinstance(hostname, six.text_type):
        # Twisted requires the hostname as decoded text, which it isn't in Python 2
        # Decode with the system encoding since this came from the config file. Die,
        # Python 2, die.
        hostname = hostname.decode(locale.getdefaultlocale()[1])
    context_factory = ssl.optionsForClientTLS(
        hostname,
        trustRoot=ca_cert or ssl.platformTrust(),
        clientCertificate=client_cert,
        extraCertificateOptions={"raiseMinimumTo": ssl.TLSVersion.TLSv1_2},
    )

    return context_factory


#: A namedtuple that represents a AMQP consumer.
#:
#: * The ``tag`` field is the consumer's AMQP tag (:class:`str`).
#: * The ``queue`` field is the name of the queue it's consuming from (:class:`str`).
#: * The ``callback`` field is the function called for each message (a callable).
#: * The ``channel`` is the AMQP channel used for the consumer
#:   (:class:`pika.adapters.twisted_connection.TwistedChannel`).
Consumer = namedtuple("Consumer", ["tag", "queue", "callback", "channel"])


class Service(service.MultiService):
    """
    A Twisted service that manages one or more client connections to an AMQP broker.

    Args:
        amqp_url (str): URL to use for the AMQP server.
    """

    def __init__(self, amqp_url):
        """Initialize the service."""
        service.MultiService.__init__(self)
        self._parameters = pika.URLParameters(amqp_url)
        if amqp_url.startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]

        self.factory = Factory(self._parameters)
        self.connection = None
        self.setName("AMQPServiceContainer")

    def startService(self):
        """
        Start the service, and by extension all its child services.

        This is part of the Twisted Service API.
        """
        # Get hold of the deferred from the protocol, register callback to exit
        # on raise exception
        if self._parameters.ssl_options:
            self.connection = SSLClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=self.factory,
                contextFactory=_ssl_context_factory(self._parameters),
            )
        else:
            self.connection = TCPClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=self.factory,
            )
        name = "{}{}:{}".format(
            "ssl:" if self._parameters.ssl_options else "",
            self._parameters.host,
            self._parameters.port,
        )
        self.connection.setName(name)
        self.connection.setServiceParent(self)

        service.MultiService.startService(self)

    @defer.inlineCallbacks
    def stopService(self):
        """
        Gracefully stop the service.

        This is part of the Twisted Service API.

        Returns:
            defer.Deferred: a Deferred which is triggered when the service has
                finished shutting down.
        """
        self.factory.stopTrying()
        yield self.factory.stopFactory()
        yield service.MultiService.stopService(self)

    def consume(self, callback, bindings, queues):
        """
        Start a consumer that lasts across individual connections.

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
            Deferred:
                A deferred object that fires with a representation of the consumer
                when it is successfully registered and running. Note that this API
                is meant to survive network problems, so consuming will continue
                until a corresponding call to :func:`consume_async_cancel` is called.
        """
        return self.factory.consume(callback, bindings, queues)


class Factory(protocol.ReconnectingClientFactory):
    """
    Reconnecting factory for the Fedora Messaging protocol.
    """

    def __init__(
        self, parameters, confirms=True, exchanges=None, queues=None, bindings=None
    ):
        """
        Create a new factory for protocol objects.

        Any exchanges, queues, bindings, or consumers provided here will be
        declared and set up each time a new protocol instance is created. In
        other words, each time a new connection is set up to the broker, it
        will start with the declaration of these objects.

        Args:
            parameters (pika.ConnectionParameters): The connection parameters.
            confirms (bool): If true, attempt to turn on publish confirms extension.
            exchanges (list of dicts): List of exchanges to declare. Each dictionary is
                passed to :meth:`pika.channel.Channel.exchange_declare` as keyword arguments,
                so any parameter to that method is a valid key.
            queues (list of dicts): List of queues to declare each dictionary is
                passed to :meth:`pika.channel.Channel.queue_declare` as keyword arguments,
                so any parameter to that method is a valid key.
            bindings (list of dicts): A list of bindings to be created between
                queues and exchanges. Each dictionary is passed to
                :meth:`pika.channel.Channel.queue_bind`. The "queue" and "exchange" keys
                are required.
        """
        self.protocol = Protocol

        self.confirms = confirms
        self._parameters = parameters
        self._client_deferred = defer.Deferred()
        self._client = None
        self._consumers = {}

    @defer.inlineCallbacks
    def consume(self, callback, bindings, queues):
        """
        Start a consumer that lasts across individual connections.

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
            Deferred:
                A deferred object that fires with a representation of the consumer
                when it is successfully registered and running. Note that this API
                is meant to survive network problems, so consuming will continue
                until a corresponding call to :func:`consume_async_cancel` is called.
        """
        expanded_bindings = []
        for binding in bindings:
            for key in binding["routing_keys"]:
                b = binding.copy()
                del b["routing_keys"]
                b["routing_key"] = key
                expanded_bindings.append(b)

        expanded_queues = []
        for name, settings in queues.items():
            q = {"queue": name}
            q.update(settings)
            expanded_queues.append(q)

        _log.info("Requesting connection")
        protocol = yield defer.maybeDeferred(self.get_connection)
        _log.info("Got connection")
        yield protocol.declare_queues(expanded_queues)
        yield protocol.bind_queues(expanded_bindings)

        consumers = []
        for queue in expanded_queues:
            consumer = yield protocol.consume(callback, queue["queue"])
            consumers.append(consumer)

        defer.returnValue(consumers)

    @defer.inlineCallbacks
    def cancel(self, consumer):
        del self._consumers[consumer["queue"]]
        protocol = yield defer.maybeDeferred(self.get_connection)
        yield protocol.cancel(consumer)

    def get_connection(self):
        if self._client and not self._client.is_closed:
            return self._client
        else:
            return self._client_deferred
        pass

    def buildProtocol(self, addr):
        """Create the Protocol instance.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        self._client = self.protocol(self._parameters, confirms=self.confirms)
        self._client.factory = self

        def on_ready(unused_param=None):
            """Reset the connection delay when the AMQP handshake is complete."""
            self.resetDelay()
            self._client_deferred.callback(self._client)
            self._client_deferred = defer.Deferred()

        self._client.ready.addCallback(on_ready)
        return self._client

    @defer.inlineCallbacks
    def stopFactory(self):
        """Stop the factory.

        See the documentation of
        `twisted.internet.protocol.ReconnectingClientFactory` for details.
        """
        if self._client:
            yield self._client.stopProducing()
        protocol.ReconnectingClientFactory.stopFactory(self)


class Protocol(TwistedProtocolConnection):
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
        while consumer["running"]:
            try:
                deferred_get = queue_object.get()
                deferred_get.addTimeout(3, reactor)
                channel, delivery_frame, properties, body = yield deferred_get
            except defer.TimeoutError:
                continue

            _log.debug(
                "Message arrived with delivery tag {tag} for {consumer}",
                tag=delivery_frame.delivery_tag,
                consumer=consumer["tag"],
            )
            try:
                message = get_message(delivery_frame.routing_key, properties, body)
                message.queue = consumer["queue"]
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
                yield threads.deferToThread(consumer["callback"], message)
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
        channel = yield self._allocate_channel()
        consumer = {
            "tag": str(uuid.uuid4()),
            "queue": queue,
            "callback": callback,
            "channel": channel,
            "running": True,
            "read_loop": None
        }
        queue_object, _ = yield channel.basic_consume(
            queue=consumer["queue"], consumer_tag=consumer["tag"]
        )
        consumer["read_loop"] = self._read(queue_object, consumer)
        self._consumers[queue] = consumer
        _log.info("Successfully registered AMQP consumer {c}", c=consumer)
        defer.returnValue(consumer)

    @defer.inlineCallbacks
    def cancel(self, consumer):
        consumer["running"] = False

        # Wait for the last message to finish.
        yield consumer["read_loop"]

        del self._consumers[consumer["queue"]]

        try:
            yield consumer["channel"].basic_cancel(consumer_tag=consumer["tag"])
        except pika.exceptions.AMQPChannelError:
            # Consumers are tied to channels, so if this channel is dead the
            # consumer should already be canceled (and we can't get to it anyway)
            pass
        try:
            yield consumer["channel"].close()
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
        # Start consuming
        for consumer in self._consumers:
            consumer["running"] = True

        self._running = True
        for consumer in self._consumers.values():
            queue_object, _ = yield consumer["channel"].basic_consume(
                queue=consumer["queue"], consumer_tag=consumer["tag"]
            )
            deferred = self._read(queue_object, consumer)
            deferred.addErrback(
                lambda f: _log.failure, "_read failed on consumer {c}", c=consumer
            )
            consumer["read_loop"] = deferred
        _log.info("AMQP connection successfully established")

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
            c["running"] = False

        read_loops = [c["read_loop"] for c in self._consumers.values()]
        _log.info(
            "Waiting for {n} consumer(s) to finish processing before canceling the consumer",
            n=len(read_loops)
        )
        yield defer.gatherResults(read_loops)
        for consumer in self._consumers.values():
            yield consumer["channel"].basic_cancel(consumer_tag=consumer["tag"])
            try:
                yield consumer["channel"].close()
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
            # We were asked to stop because the connection is being restarted.
            # There's no graceful way to stop because we can't acknowledge messages
            # in the middle of being processed.
            self._channel = None
            return
        yield self.pauseProducing()
        yield self.close()
        self._consumers = {}
        self._channel = None
