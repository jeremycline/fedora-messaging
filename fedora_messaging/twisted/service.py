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
import locale

import pika
import six
from twisted.application import service
from twisted.application.internet import TCPClient, SSLClient
from twisted.internet import ssl, defer

from .. import config
from .._session import _configure_tls_parameters
from .factory import FedoraMessagingFactory


class FedoraMessagingService(service.MultiService):
    """
    A Twisted service to connect to the Fedora Messaging broker.

    Args:
        on_message (callable|None): Callback that will be passed each
            incoming messages. If None, no message consuming is setup.
        amqp_url (str): URL to use for the AMQP server.
        exchanges (list of dicts): List of exchanges to declare at the start of
            every connection. Each dictionary is passed to
            :meth:`pika.channel.Channel.exchange_declare` as keyword arguments,
            so any parameter to that method is a valid key.
        queues (list of dicts): List of queues to declare at the start of every
            connection. Each dictionary is passed to
            :meth:`pika.channel.Channel.queue_declare` as keyword arguments,
            so any parameter to that method is a valid key.
        bindings (list of dicts): A list of bindings to be created between
            queues and exchanges. Each dictionary is passed to
            :meth:`pika.channel.Channel.queue_bind`. The "queue" and "exchange" keys
            are required.
        consumers (dict): A dictionary where each key is a queue name and the value
            is a callable object to handle messages on that queue. Consumers will be
            set up after each connection is established so they will survive networking
            issues.
    """

    name = "fedora-messaging"
    factoryClass = FedoraMessagingFactory

    def __init__(
        self, amqp_url=None, exchanges=None, queues=None, bindings=None, consumers=None
    ):
        """Initialize the service."""
        service.MultiService.__init__(self)
        amqp_url = amqp_url or config.conf["amqp_url"]
        self._parameters = pika.URLParameters(amqp_url)
        if amqp_url.startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]
        self._exchanges = exchanges or []
        self._queues = queues or []
        self._bindings = bindings or []
        self.factory = self.factoryClass(
            self._parameters, exchanges=exchanges, queues=queues, bindings=bindings
        )
        self._consumers = []
        consumers = consumers or {}
        for queue, callback in consumers.items():
            self._consumers.append(self.factory.consume(callback, queue))

    def startService(self):
        # Get hold of the deferred from the protocol, register callback to exit
        # on raise exception
        self.connect()
        service.MultiService.startService(self)

    @defer.inlineCallbacks
    def stopService(self):
        """
        Gracefully stop the service.

        Returns:
            defer.Deferred: a Deferred which is triggered when the service has
                finished shutting down.
        """
        self.factory.stopTrying()
        yield self.factory.stopFactory()
        yield service.MultiService.stopService(self)

    def getFactory(self):
        return self.factory

    def connect(self):
        if self._parameters.ssl_options:
            serv = SSLClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=self.factory,
                contextFactory=_ssl_context_factory(self._parameters),
            )
        else:
            serv = TCPClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=self.factory,
            )
        serv.factory = self.factory
        name = "{}{}:{}".format(
            "ssl:" if self._parameters.ssl_options else "",
            self._parameters.host,
            self._parameters.port,
        )
        serv.setName(name)
        serv.setServiceParent(self)


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
