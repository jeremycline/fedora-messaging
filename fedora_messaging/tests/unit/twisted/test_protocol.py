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

import json
import unittest

import mock
import pika
import pkg_resources
import pytest
from twisted.internet import defer, error

from fedora_messaging import config
from fedora_messaging.message import Message
from fedora_messaging.exceptions import Nack, Drop, HaltConsumer
from fedora_messaging.twisted.protocol import FedoraMessagingProtocol, _pika_version


try:
    import pytest_twisted
except ImportError:
    pytest.skip("pytest-twisted is missing, skipping tests", allow_module_level=True)


class MockChannel(mock.Mock):
    """A mock object with Channel-specific methods that return Deferreds."""

    def __init__(self, *args, **kwargs):
        super(MockChannel, self).__init__(*args, **kwargs)
        deferred_methods = (
            "basic_qos",
            "confirm_delivery",
            "exchange_declare",
            "queue_bind",
            "basic_ack",
            "basic_nack",
            "basic_publish",
        )
        for method in deferred_methods:
            setattr(
                self,
                method,
                mock.Mock(side_effect=lambda *a, **kw: defer.succeed(None)),
            )
        self.queue_declare = mock.Mock(
            side_effect=lambda **kw: defer.succeed(
                pika.frame.Method(0, pika.spec.Queue.DeclareOk(queue=kw["queue"]))
            )
        )
        self.basic_consume = mock.Mock(
            side_effect=lambda **kw: defer.succeed(
                (mock.Mock(name="queue_object"), "consumer-tag")
            )
        )


class MockProtocol(FedoraMessagingProtocol):
    """A Protocol object that mocks the underlying channel and impl."""

    def __init__(self, *args, **kwargs):
        super(MockProtocol, self).__init__(*args, **kwargs)
        self._impl = mock.Mock(name="_impl")
        self._impl.is_closed = True
        self._channel = MockChannel(name="_channel")
        self.channel = mock.Mock(
            name="channel", side_effect=lambda: defer.succeed(self._channel)
        )
        self._running = False


class ProtocolTests(unittest.TestCase):
    def setUp(self):
        self.protocol = MockProtocol(None)
        self.factory = mock.Mock()
        self.protocol.factory = self.factory

    def test_connection_ready(self):
        # Check the ready Deferred.
        def _check(_):
            self.protocol.channel.assert_called()
            self.protocol._channel.basic_qos.assert_called_with(
                prefetch_count=config.conf["qos"]["prefetch_count"],
                prefetch_size=config.conf["qos"]["prefetch_size"],
            )
            if _pika_version >= pkg_resources.parse_version("1.0.0b1"):
                self.protocol._channel.confirm_delivery.assert_called()

        d = self.protocol.ready
        d.addCallback(_check)

        if _pika_version < pkg_resources.parse_version("1.0.0b1"):
            self.protocol.connectionReady(None)
        else:
            self.protocol._on_connection_ready(None)

        return pytest_twisted.blockon(d)

    def test_setup_read(self):
        # Check the setupRead method.
        self.factory.bindings = [
            {"exchange": "testexchange1", "queue": "testqueue1", "routing_keys": ["#"]},
            {
                "exchange": "testexchange2",
                "queue": "testqueue2",
                "queue_auto_delete": True,
                "routing_keys": ["testrk"],
            },
            {
                "exchange": "testexchange3",
                "queue": "testqueue3",
                "routing_keys": ["#"],
                "queue_arguments": {},
            },
        ]
        callback = mock.Mock()
        d = self.protocol.setupRead(callback)

        def _check(_):
            self.assertTrue(self.protocol._message_callback is callback)
            self.assertEqual(
                self.protocol._queues, set(["testqueue1", "testqueue2", "testqueue3"])
            )
            self.assertEqual(
                self.protocol._channel.exchange_declare.call_args_list,
                [
                    (
                        (),
                        dict(
                            exchange="testexchange1",
                            exchange_type="topic",
                            durable=True,
                        ),
                    ),
                    (
                        (),
                        dict(
                            exchange="testexchange2",
                            exchange_type="topic",
                            durable=True,
                        ),
                    ),
                    (
                        (),
                        dict(
                            exchange="testexchange3",
                            exchange_type="topic",
                            durable=True,
                        ),
                    ),
                ],
            )
            self.assertEqual(
                self.protocol._channel.queue_declare.call_args_list,
                [
                    (
                        (),
                        dict(
                            queue="testqueue1",
                            arguments=None,
                            auto_delete=False,
                            durable=True,
                        ),
                    ),
                    (
                        (),
                        dict(
                            queue="testqueue2",
                            arguments=None,
                            auto_delete=True,
                            durable=True,
                        ),
                    ),
                    (
                        (),
                        dict(
                            queue="testqueue3",
                            arguments={},
                            auto_delete=False,
                            durable=True,
                        ),
                    ),
                ],
            )
            self.assertEqual(
                self.protocol._channel.queue_bind.call_args_list,
                [
                    (
                        (),
                        dict(
                            exchange="testexchange1",
                            queue="testqueue1",
                            routing_key="#",
                        ),
                    ),
                    (
                        (),
                        dict(
                            exchange="testexchange2",
                            queue="testqueue2",
                            routing_key="testrk",
                        ),
                    ),
                    (
                        (),
                        dict(
                            exchange="testexchange3",
                            queue="testqueue3",
                            routing_key="#",
                        ),
                    ),
                ],
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_setupRead_no_bindings(self):
        # The setupRead method should do nothing when there are no bindings in
        # the factory.
        self.factory.bindings = []
        callback = mock.Mock()
        d = self.protocol.setupRead(callback)

        def _check(_):
            self.assertIsNone(self.protocol._message_callback)
            self.protocol._channel.exchange_declare.assert_not_called()
            self.protocol._channel.queue_declare.assert_not_called()
            self.protocol._channel.queue_bind.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_publish(self):
        # Check the publish method.
        body = {"bodykey": "bodyvalue"}
        headers = {"headerkey": "headervalue"}
        message = Message(body, headers, "testing.topic")
        d = self.protocol.publish(message, "test-exchange")

        def _check(_):
            self.protocol._channel.basic_publish.assert_called_once()
            args = self.protocol._channel.basic_publish.call_args_list[0][1]
            self.assertEqual(args["exchange"], "test-exchange")
            self.assertEqual(args["routing_key"], b"testing.topic")
            self.assertEqual(args["body"], json.dumps(body).encode("utf-8"))
            props = args["properties"]
            self.assertEqual(props.headers, headers)
            self.assertEqual(props.content_encoding, "utf-8")
            self.assertEqual(props.content_type, "application/json")
            self.assertEqual(props.delivery_mode, 2)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_resumeProducing(self):
        # Check the resumeProducing method.
        self.protocol._running = False
        self.protocol._queues = set(["testqueue1", "testqueue2", "testqueue3"])
        self.protocol._read = mock.Mock(side_effect=lambda _: defer.succeed(None))
        d = self.protocol.resumeProducing()

        def _check(_):
            self.assertTrue(self.protocol._running)
            self.assertEqual(self.protocol._channel.basic_consume.call_count, 3)
            called_queues = [
                kw["queue"]
                for arg, kw in self.protocol._channel.basic_consume.call_args_list
            ]
            self.assertEqual(set(called_queues), self.protocol._queues)
            self.assertEqual(self.protocol._read.call_count, 3)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_pauseProducing(self):
        # Check the pauseProducing method.
        self.protocol._running = True
        self.protocol._channel.consumer_tags = ["ct1", "ct2"]
        d = self.protocol.pauseProducing()

        def _check(_):
            self.assertFalse(self.protocol._running)
            self.assertEqual(self.protocol._channel.basic_cancel.call_count, 2)
            called_cts = [
                kw["consumer_tag"]
                for arg, kw in self.protocol._channel.basic_cancel.call_args_list
            ]
            self.assertEqual(called_cts, ["ct1", "ct2"])

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_pauseProducing_not_running(self):
        # The pauseProducing method should do nothing when the protocol is not
        # consuming messages..
        self.protocol._channel.consumer_tags = ["ct1", "ct2"]
        self.protocol._running = False
        d = self.protocol.pauseProducing()

        def _check(_):
            self.protocol._channel.basic_cancel.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_pauseProducing_no_channel(self):
        # The pauseProducing method should do nothing when the protocol has no
        # channel yet.
        self.protocol._channel = None
        d = self.protocol.pauseProducing()
        return pytest_twisted.blockon(d)

    def test_stopProducing(self):
        # Check the stopProducing method.
        self.protocol._channel.consumer_tags = []
        self.protocol.close = mock.Mock()
        self.protocol._running = True
        self.protocol._impl.is_closed = False
        d = self.protocol.stopProducing()

        def _check(_):
            self.assertFalse(self.protocol._running)
            self.protocol.close.assert_called()
            self.assertIsNone(self.protocol._channel)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_stopProducing_no_channel(self):
        # The stopProducing method should do nothing when the protocol has no
        # channel yet.
        self.protocol._channel = None
        self.protocol.close = mock.Mock()
        d = self.protocol.stopProducing()

        def _check(_):
            self.protocol.close.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class ProtocolReadTests(unittest.TestCase):
    """Test the _read() method on the Protocol."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self.protocol._running = True
        self.protocol._impl.is_closed = False
        self.protocol._on_message = mock.Mock()
        self.queue = mock.Mock()

    def test_read_not_running(self):
        # When not running, _read() should do nothing.
        self.queue.get.side_effect = lambda: defer.succeed(None)
        self.protocol._running = False
        d = self.protocol._read(self.queue)

        def _check(_):
            self.queue.get.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read(self):
        # Check the nominal case for _read().
        self.queue.get.side_effect = [
            defer.succeed((None, "df1", "prop1", "body1")),
            defer.succeed((None, "df2", "prop2", "body2")),
            defer.succeed((None, "df3", "prop3", "body3")),
            pika.exceptions.ChannelClosed(42, "testing"),
        ]

        def _check(_):
            self.assertEqual(
                self.protocol._on_message.call_args_list,
                [
                    (("df1", "prop1", "body1"), {}),
                    (("df2", "prop2", "body2"), {}),
                    (("df3", "prop3", "body3"), {}),
                ],
            )

        d = self.protocol._read(self.queue)
        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_no_body(self):
        # When a message has no body, it should not be passed to the callback.
        self.queue.get.side_effect = [
            defer.succeed((None, None, None, None)),
            defer.succeed((None, None, None, "")),
        ]
        d = self.protocol._read(self.queue)

        def _check(_):
            self.protocol._on_message.assert_not_called()

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_read_exit_loop(self):
        # Check the exceptions that cause the read loop to exit.
        exceptions = [
            error.ConnectionDone(),
            pika.exceptions.ChannelClosed(42, "testing"),
            pika.exceptions.ConsumerCancelled(),
            RuntimeError(),
        ]
        if _pika_version >= pkg_resources.parse_version("1.0.0b1"):
            exceptions.append(pika.exceptions.ChannelClosedByClient(42, "testing"))

        deferreds = []
        for exc in exceptions:
            queue = mock.Mock()
            queue.get.side_effect = exc
            deferreds.append(self.protocol._read(queue))

        def _check(_):
            self.protocol._on_message.assert_not_called()

        d = defer.DeferredList(deferreds)
        d.addCallback(_check)
        return pytest_twisted.blockon(d)


class ProtocolOnMessageTests(unittest.TestCase):
    """Test the _on_message() method on the Protocol."""

    def setUp(self):
        self.protocol = MockProtocol(None)
        self.protocol._message_callback = mock.Mock()
        self.protocol._impl.is_closed = False

    def _call_on_message(self, topic, headers, body):
        """Prepare arguments for the _on_message() method and call it."""
        full_headers = {"fedora_messaging_schema": "fedora_messaging.message:Message"}
        full_headers.update(headers)
        return self.protocol._on_message(
            pika.spec.Basic.Deliver(routing_key=topic, delivery_tag="delivery_tag"),
            pika.spec.BasicProperties(headers=full_headers),
            json.dumps(body).encode("utf-8"),
        )

    def test_on_message(self):
        # Check the nominal case.
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.protocol._message_callback.assert_called()
            self.protocol._channel.basic_ack.assert_called_with(
                delivery_tag="delivery_tag"
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_invalid(self):
        # When a message is invalid, it should be Nacked.
        d = self._call_on_message("testing.topic", {}, "testing")

        def _check(_):
            self.protocol._message_callback.assert_not_called()
            self.protocol._channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_nack(self):
        # When the callback raises a Nack, the server should be notified.
        self.protocol._message_callback.side_effect = Nack()
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.protocol._message_callback.assert_called()
            self.protocol._channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=True
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_drop(self):
        # When the callback raises a Drop, the server should be notified.
        self.protocol._message_callback.side_effect = Drop()
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.protocol._message_callback.assert_called()
            self.protocol._channel.basic_nack.assert_called_with(
                delivery_tag="delivery_tag", requeue=False
            )

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_halt(self):
        # When the callback raises a HaltConsumer exception, the consumer
        # should stop.
        self.protocol._message_callback.side_effect = HaltConsumer()
        channel = self.protocol._channel
        self.protocol.close = mock.Mock()
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.protocol._message_callback.assert_called()
            channel.basic_ack.assert_called_with(delivery_tag="delivery_tag")
            self.assertFalse(self.protocol._running)
            self.protocol.close.assert_called()
            self.assertIsNone(self.protocol._channel)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)

    def test_on_message_exception(self):
        # On an unknown exception, the consumer should stop and all
        # unacknowledged messages should be requeued.
        self.protocol._message_callback.side_effect = ValueError()
        channel = self.protocol._channel
        self.protocol.close = mock.Mock()
        d = self._call_on_message("testing.topic", {}, {"key": "value"})

        def _check(_):
            self.protocol._message_callback.assert_called()
            channel.basic_nack.assert_called_with(
                delivery_tag=0, multiple=True, requeue=True
            )
            self.assertFalse(self.protocol._running)
            self.protocol.close.assert_called()
            self.assertIsNone(self.protocol._channel)

        d.addCallback(_check)
        return pytest_twisted.blockon(d)
