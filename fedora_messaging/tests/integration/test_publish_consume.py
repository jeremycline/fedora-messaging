"""Test the publish and consume APIs on a real broker running on localhost."""

from collections import defaultdict
import subprocess
import time
import unittest
import socket

import mock
import six

from fedora_messaging import api, message, exceptions, config


def cb(message, storage=defaultdict(int)):
    storage[message.topic] += 1
    if storage[message.topic] == 3:
        raise exceptions.HaltConsumer()


class PubSubTests(unittest.TestCase):
    @unittest.skipIf(six.PY2, "Python 2 doesn't support timeout on Popen.communicate")
    def test_pub_sub_default_settings(self):
        """
        Assert publishing and subscribing works with the default configuration.

        This should work because the publisher uses the 'amq.topic' exchange by
        default and the consumer also uses the 'amq.topic' exchange with its
        auto-named queue and a default subscription key of '#'.
        """

        result = subprocess.Popen(
            [
                "fedora-messaging",
                "consume",
                "--callback=fedora_messaging.tests.integration.test_publish_consume:cb",
            ]
        )
        msg = message.Message(
            topic=u"nice.message",
            headers={u"niceness": u"very"},
            body={u"encouragement": u"You're doing great!"},
        )
        # Allow the consumer time to create the queues and bindings
        time.sleep(5)

        for _ in range(0, 3):
            try:
                api.publish(msg)
            except exceptions.ConnectionException:
                result.kill()
                self.fail("Failed to publish message, is the broker running?")

        # Python 2 doesn't have a timeout
        result.communicate(timeout=30)
        self.assertEqual(0, result.returncode)

    def test_pub_connection_refused(self):
        """Assert ConnectionException is raised on connection refused."""
        # Because we don't call accept, we can be sure of a connection refusal
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("", 0))
        url = "amqp://localhost:{port}/".format(port=sock.getsockname()[1])

        with mock.patch.dict(config.conf, {"amqp_url": url}):
            self.assertRaises(
                exceptions.ConnectionException, api.publish, api.Message(), None, 3.0
            )
