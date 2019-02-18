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
A Twisted Factory for creating and configuring instances of the
:class:`.FedoraMessagingProtocol`.

A factory is used to implement automatic re-connections by producing protocol
instances (connections) on demand. Twisted uses factories for its services APIs.

See the `Twisted client
<https://twistedmatrix.com/documents/current/core/howto/clients.html#clientfactory>`_
documentation for more information.
"""

from __future__ import absolute_import

import pika
from twisted.internet import defer, protocol, error

from twisted.logger import Logger

from ..exceptions import ConnectionException
from .protocol import FedoraMessagingProtocol

_log = Logger(__name__)


class FedoraMessagingFactory(protocol.ReconnectingClientFactory):
    """Reconnecting factory for the Fedora Messaging protocol."""

    name = u"FedoraMessaging:Factory"
    protocol = FedoraMessagingProtocol

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
        self._parameters = parameters
        self.confirms = confirms
        self.exchanges = exchanges or []
        self.queues = queues or []
        self.bindings = bindings or []
        self.consumers = {}
        self.client = None
        self._client_ready = defer.Deferred()

        self.protocol = FedoraMessagingProtocol
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
