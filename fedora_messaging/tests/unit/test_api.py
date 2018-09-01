# coding: utf-8

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
"""Tests for the :module:`fedora_messaging.api` module."""

import unittest
import mock

from fedora_messaging import api
from fedora_messaging.exceptions import PublishException
from fedora_messaging.signals import (
    pre_publish_signal,
    publish_signal,
    publish_failed_signal,
)


class ConsumeTests(unittest.TestCase):
    @mock.patch("fedora_messaging._session.ConsumerSession")
    def test_bindings_are_dict(self, mock_session):
        """Assert consume is working(bindings type is dict)"""
        mock_session.return_value = mock_session
        api.consume("test_callback", dict())
        mock_session.assert_called_once()
        mock_session.consume.assert_called_once_with("test_callback", [dict()])

    @mock.patch("fedora_messaging._session.ConsumerSession")
    def test_bindings_not_dict(self, mock_session):
        """Assert consume is working(bindings type is not dict)"""
        mock_session.return_value = mock_session
        api.consume("test_callback", "test_bindings")
        mock_session.assert_called_once()
        mock_session.consume.assert_called_once_with("test_callback", "test_bindings")


class PublishTests(unittest.TestCase):
    def setUp(self):
        self.pre_publish_signal_data = {"called": False, "sender": None, "args": None}
        self.publish_signal_data = {"called": False, "sender": None, "args": None}
        self.publish_failed_signal_data = {
            "called": False,
            "sender": None,
            "args": None,
        }

        @pre_publish_signal.connect
        def pre_publish_signal_handler(sender, **kwargs):
            self.pre_publish_signal_data = {
                "called": True,
                "sender": sender,
                "args": kwargs,
            }

        @publish_signal.connect
        def publish_signal_handler(sender, **kwargs):
            self.publish_signal_data = {
                "called": True,
                "sender": sender,
                "args": kwargs,
            }

        @publish_failed_signal.connect
        def publish_failed_signal_handler(sender, **kwargs):
            self.publish_failed_signal_data = {
                "called": True,
                "sender": sender,
                "args": kwargs,
            }

        self.pre_publish_signal_handler = pre_publish_signal_handler
        self.publish_signal_handler = publish_signal_handler
        self.publish_failed_signal_handler = publish_failed_signal_handler

        self.session_cache_patch = mock.patch("fedora_messaging.api._session_cache")
        self.publisher_session_patch = mock.patch(
            "fedora_messaging._session.PublisherSession"
        )
        self.mock_session_cache = self.session_cache_patch.start()
        self.mock_publisher_session = self.publisher_session_patch.start()
        self.mock_publisher_session.return_value = self.mock_publisher_session
        del self.mock_session_cache.session

    def tearDown(self):
        pre_publish_signal.disconnect(self.pre_publish_signal_handler)
        publish_signal.disconnect(self.publish_signal_handler)
        publish_failed_signal.disconnect(self.publish_failed_signal_handler)
        mock.patch.stopall()

    def test_publish_to_exchange(self):
        """Assert a message can be published to the exchange."""
        message = "test_message"
        exchange = "test_exchange"
        expected_pre_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_failed_signal_data = {
            "called": False,
            "sender": None,
            "args": None,
        }
        api.publish(message, exchange)
        self.mock_publisher_session.assert_called_once_with()
        self.mock_publisher_session.publish.assert_called_once_with(
            message, exchange=exchange
        )
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
        )

    @mock.patch.dict(
        "fedora_messaging.config.conf", {"publish_exchange": "test_publich_exchange"}
    )
    def test_publish_to_config_exchange(self):
        """Assert a message can be published to the exchange form config."""
        message = "test_message"
        expected_pre_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_failed_signal_data = {
            "called": False,
            "sender": None,
            "args": None,
        }
        api.publish(message)
        self.mock_publisher_session.assert_called_once_with()
        self.mock_publisher_session.publish.assert_called_once_with(
            message, exchange="test_publich_exchange"
        )
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
        )

    def test_session_cache_has_session(self):
        """Assert a TLS vaiable _session_cache contains 'session'."""
        message = "test_message"
        exchange = "test_exchange"
        expected_pre_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_failed_signal_data = {
            "called": False,
            "sender": None,
            "args": None,
        }
        self.mock_session_cache.session = self.mock_publisher_session
        api.publish(message, exchange)
        self.mock_publisher_session.assert_not_called()
        self.mock_publisher_session.publish.assert_called_once_with(
            message, exchange=exchange
        )
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
        )

    def test_publish_failed(self):
        """Assert an exception is raised when message can't be published."""
        message = "test_message"
        exchange = "test_exchange"
        expected_exception = PublishException(reason="Unable to publish message")
        expected_pre_publish_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message},
        }
        expected_publish_signal_data = {"called": False, "sender": None, "args": None}
        expected_publish_failed_signal_data = {
            "called": True,
            "sender": api.publish,
            "args": {"message": message, "reason": expected_exception},
        }
        self.mock_publisher_session.publish.side_effect = expected_exception
        self.assertRaises(
            type(expected_exception), api.publish, message, exchange=exchange
        )
        self.mock_publisher_session.assert_called_once_with()
        self.mock_publisher_session.publish.assert_called_once_with(
            message, exchange=exchange
        )
        self.assertEqual(self.pre_publish_signal_data, expected_pre_publish_signal_data)
        self.assertEqual(self.publish_signal_data, expected_publish_signal_data)
        self.assertEqual(
            self.publish_failed_signal_data, expected_publish_failed_signal_data
        )
