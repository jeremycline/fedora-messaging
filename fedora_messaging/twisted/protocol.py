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
Twisted Protocol based on pika's Twisted Protocol, and implementing the
specificities of Fedora Messaging.

See https://twistedmatrix.com/documents/current/core/howto/clients.html#protocol
"""

from __future__ import absolute_import

import logging

import pika
import pkg_resources
from pika.adapters.twisted_connection import TwistedProtocolConnection
from twisted.internet import defer, error

# twisted.logger is available with Twisted 15+
from twisted.python import log

from .. import config
from ..message import get_message
from ..exceptions import Nack, Drop, HaltConsumer, ValidationError


_pika_version = pkg_resources.get_distribution("pika").parsed_version
if _pika_version < pkg_resources.parse_version("1.0.0b1"):
    ChannelClosedByClient = pika.exceptions.ChannelClosed
else:
    ChannelClosedByClient = pika.exceptions.ChannelClosedByClient


class FedoraMessagingProtocol(TwistedProtocolConnection):
    """A Twisted Protocol for the Fedora Messaging system.

    This protocol builds on the generic pika AMQP protocol to add calls
    specific to the Fedora Messaging implementation.
    """

    name = u"FedoraMessaging:Protocol"

    def __init__(self, parameters, confirms=True):
        """Initialize the protocol.

        Args:
            parameters (pika.ConnectionParameters): The connection parameters.
            confirms (bool): If True, all outgoing messages will require a
                confirmation from the server, and the Deferred returned from
                the publish call will wait for that confirmation.
        """
        TwistedProtocolConnection.__init__(self, parameters)
        self._parameters = parameters
        if confirms and _pika_version < pkg_resources.parse_version("1.0.0b1"):
            log.msg(
                "Message confirmation is only available with pika 1.0.0+",
                system=self.name,
                logLevel=logging.ERROR,
            )
            confirms = False
        self._confirms = confirms
        self._channel = None
        self._running = False
        self._queues = set()
        self._message_callback = None
        self._halted = defer.Deferred()
        self.factory = None

    @defer.inlineCallbacks
    def connectionReady(self, res=None):
        """Called when the AMQP connection is ready.
        """
        # The optional `res` argument is for compatibility with pika < 1.0.0
        # Create channel
        self._channel = yield self.channel()
        log.msg("AMQP channel created", system=self.name, logLevel=logging.DEBUG)
        yield self._channel.basic_qos(
            prefetch_count=config.conf["qos"]["prefetch_count"],
            prefetch_size=config.conf["qos"]["prefetch_size"],
        )
        if self._confirms:
            yield self._channel.confirm_delivery()
        if _pika_version < pkg_resources.parse_version("1.0.0b1"):
            TwistedProtocolConnection.connectionReady(self, res)

    @defer.inlineCallbacks
    def setupRead(self, message_callback):
        """Pass incoming messages to the provided callback.

        Args:
            message_callback (callable): The callable to pass the message to
                when one arrives.
        """
        if not self.factory.bindings:
            return
        self._message_callback = message_callback
        for binding in self.factory.bindings:
            yield self._channel.exchange_declare(
                exchange=binding["exchange"], exchange_type="topic", durable=True
            )
            result = yield self._channel.queue_declare(
                queue=binding["queue"],
                durable=True,
                auto_delete=binding.get("queue_auto_delete", False),
                arguments=binding.get("queue_arguments"),
            )
            queue_name = result.method.queue
            for routing_key in binding["routing_keys"]:
                yield self._channel.queue_bind(
                    queue=queue_name,
                    exchange=binding["exchange"],
                    routing_key=routing_key,
                )
            self._queues.add(queue_name)
        log.msg("AMQP bindings declared", system=self.name, logLevel=logging.INFO)

    @defer.inlineCallbacks
    def _read(self, queue_object):
        while self._running:
            try:
                _channel, delivery_frame, properties, body = yield queue_object.get()
            except (error.ConnectionDone, ChannelClosedByClient):
                # This is deliberate.
                log.msg(
                    "Closing the read loop on the producer.",
                    system=self.name,
                    logLevel=logging.DEBUG,
                )
                break
            except pika.exceptions.ChannelClosed as e:
                log.err(e, system=self.name)
                break
            except pika.exceptions.ConsumerCancelled:
                log.msg("Consumer cancelled, quitting the read loop.", system=self.name)
                break
            except Exception as e:
                log.err(
                    "Failed getting the next message in the queue, " "stopping.",
                    system=self.name,
                )
                log.err(e, system=self.name)
                break
            if body:
                yield self._on_message(delivery_frame, properties, body)

    @defer.inlineCallbacks
    def _on_message(self, delivery_frame, properties, body):
        """
        Callback when a message is received from the server.

        This method wraps a user-registered callback for message delivery. It
        decodes the message body, determines the message schema to validate the
        message with, and validates the message before passing it on to the
        user callback.

        This also handles acking, nacking, and rejecting messages based on
        exceptions raised by the consumer callback. For detailed documentation
        on the user-provided callback, see the user guide on consuming.

        Args:
            delivery_frame (pika.spec.Deliver): The delivery frame which
                includes details about the message like content encoding and
                its delivery tag.
            properties (pika.spec.BasicProperties): The message properties like
                the message headers.
            body (bytes): The message payload.

        Returns:
            Deferred: fired when the message has been handled.
        """
        log.msg(
            "Message arrived with delivery tag {tag}".format(
                tag=delivery_frame.delivery_tag
            ),
            system=self.name,
            logLevel=logging.DEBUG,
        )
        try:
            message = get_message(delivery_frame.routing_key, properties, body)
        except ValidationError:
            log.msg(
                "Message id {msgid} did not pass validation.".format(
                    msgid=properties.message_id
                ),
                system=self.name,
                logLevel=logging.WARNING,
            )
            yield self._channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
            return

        try:
            log.msg(
                "Consuming message from topic {topic!r} (id {msgid})".format(
                    topic=message.topic, msgid=properties.message_id
                ),
                system=self.name,
                logLevel=logging.DEBUG,
            )
            yield defer.maybeDeferred(self._message_callback, message)
        except Nack:
            log.msg(
                "Returning message id {msgid} to the queue".format(
                    msgid=properties.message_id
                ),
                system=self.name,
                logLevel=logging.WARNING,
            )
            yield self._channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=True
            )
        except Drop:
            log.msg(
                "Dropping message id {msgid}".format(msgid=properties.message_id),
                system=self.name,
                logLevel=logging.WARNING,
            )
            yield self._channel.basic_nack(
                delivery_tag=delivery_frame.delivery_tag, requeue=False
            )
        except HaltConsumer as e:
            log.msg(
                "Consumer indicated it wishes consumption to halt, " "shutting down",
                system=self.name,
                logLevel=logging.WARNING,
            )
            yield self._channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)
            yield self._halted.callback(e)
            yield self.stopProducing()
        except Exception:
            log.err(
                "Received unexpected exception from consumer callback", system=self.name
            )
            log.err(system=self.name)
            yield self._channel.basic_nack(delivery_tag=0, multiple=True, requeue=True)
            yield self.stopProducing()
        else:
            yield self._channel.basic_ack(delivery_tag=delivery_frame.delivery_tag)

    @defer.inlineCallbacks
    def publish(self, message, exchange):
        """
        Publish a :class:`fedora_messaging.message.Message` to an `exchange`_
        on the message broker.

        Args:
            message (message.Message): The message to publish.
            exchange (str): The name of the AMQP exchange to publish to

        .. _exchange: https://www.rabbitmq.com/tutorials/amqp-concepts.html#exchanges
        """
        message.validate()
        yield self._channel.basic_publish(
            exchange=exchange,
            routing_key=message._encoded_routing_key,
            body=message._encoded_body,
            properties=message._properties,
        )

    @defer.inlineCallbacks
    def resumeProducing(self):
        """
        Starts or resumes the retrieval of messages from the server queue.

        This method starts receiving messages from the server, they will be
        passed to the consumer callback.

        Returns:
            Deferred: fired when the production is ready to start
        """
        # Start consuming
        self._running = True
        for queue_name in self._queues:
            queue_object, _consumer_tag = yield self._channel.basic_consume(
                queue=queue_name
            )
            self._read(queue_object).addErrback(log.err, system=self.name)
        log.msg("AMQP consumer is ready", system=self.name, logLevel=logging.DEBUG)

    @defer.inlineCallbacks
    def pauseProducing(self):
        """
        Pause the reception of messages. Does not disconnect from the server.

        Message reception can be resumed with :meth:`resumeProducing`.

        Returns:
            Deferred: fired when the production is paused.
        """
        if self._channel is None:
            return
        if not self._running:
            return
        # Exit the read loop and cancel the consumer on the server.
        self._running = False
        for consumer_tag in self._channel.consumer_tags:
            yield self._channel.basic_cancel(consumer_tag=consumer_tag)
        log.msg(
            "Paused retrieval of messages for the server queue",
            system=self.name,
            logLevel=logging.DEBUG,
        )

    @defer.inlineCallbacks
    def stopProducing(self):
        """
        Stop producing messages and disconnect from the server.

        Returns:
            Deferred: fired when the production is stopped.
        """
        if self._channel is None:
            return
        if self._running:
            yield self.pauseProducing()
        if not self.is_closed:
            log.msg(
                "Disconnecting from the Fedora Messaging broker",
                system=self.name,
                logLevel=logging.DEBUG,
            )
            yield self.close()
        self._channel = None
