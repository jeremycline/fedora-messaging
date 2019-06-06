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
import logging
import ssl
import warnings

from pika import SSLOptions
import pika
import six
from twisted.application import service
from twisted.application.internet import TCPClient, SSLClient
from twisted.internet import ssl as twisted_ssl, defer

from .. import config, exceptions
from .factory import FedoraMessagingFactory, FedoraMessagingFactoryV2


_log = logging.getLogger(__name__)


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
        consumers = consumers or {}
        for queue, callback in consumers.items():
            self.factory.consume(callback, queue)
        warnings.warn(
            "The FedoraMessagingService class is deprecated and will be removed"
            " in fedora-messaging v2.0, please use FedoraMessagingServiceV2 instead.",
            DeprecationWarning,
        )

    def startService(self):
        self.connect()
        service.MultiService.startService(self)

    def stopService(self):
        factory = self.getFactory()
        if not factory:
            return
        factory.stopTrying()
        service.MultiService.stopService(self)

    def getFactory(self):
        if self.services:
            return self.services[0].factory
        return None

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


class FedoraMessagingServiceV2(service.MultiService):
    """
    A Twisted service to connect to the Fedora Messaging broker.

    Args:
        amqp_url (str): URL to use for the AMQP server.
        publish_confirms (bool): If true, use the RabbitMQ publisher confirms
            AMQP extension.
    """

    name = "fedora-messaging-servicev2"

    def __init__(self, amqp_url=None, publish_confirms=True):
        """Initialize the service."""
        service.MultiService.__init__(self)
        self._parameters = pika.URLParameters(amqp_url or config.conf["amqp_url"])
        self._confirms = publish_confirms
        if amqp_url.startswith("amqps"):
            _configure_tls_parameters(self._parameters)
        if self._parameters.client_properties is None:
            self._parameters.client_properties = config.conf["client_properties"]

        factory = FedoraMessagingFactoryV2(self._parameters, self._confirms)
        if self._parameters.ssl_options:
            self._service = SSLClient(
                host=self._parameters.host,
                port=self._parameters.port,
                factory=factory,
                contextFactory=_ssl_context_factory(self._parameters),
            )
        else:
            self._service = TCPClient(
                host=self._parameters.host, port=self._parameters.port, factory=factory
            )
        self._service.factory = factory
        name = "{}{}:{}".format(
            "ssl:" if self._parameters.ssl_options else "",
            self._parameters.host,
            self._parameters.port,
        )
        self._service.setName(name)
        self._service.setServiceParent(self)

    @defer.inlineCallbacks
    def stopService(self):
        """
        Gracefully stop the service.

        Returns:
            defer.Deferred: a Deferred which is triggered when the service has
                finished shutting down.
        """
        self._service.factory.stopTrying()
        yield self._service.factory.stopFactory()
        yield service.MultiService.stopService(self)


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

    ssl_context = ssl.create_default_context()
    if config.conf["tls"]["ca_cert"]:
        try:
            ssl_context.load_verify_locations(cafile=config.conf["tls"]["ca_cert"])
        except ssl.SSLError as e:
            raise exceptions.ConfigurationException(
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
            raise exceptions.ConfigurationException(
                'The "keyfile" setting in the "tls" section is invalid ({})'.format(e)
            )
    parameters.ssl_options = SSLOptions(ssl_context, server_hostname=parameters.host)


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
            ca_cert = twisted_ssl.Certificate.loadPEM(fd.read())
    if key and cert:
        # Note that _configure_tls_parameters sets the auth mode to EXTERNAL
        # if both key and cert are defined, so we don't need to do that here.
        with open(key) as fd:
            client_keypair = fd.read()
        with open(cert) as fd:
            client_keypair += fd.read()
        client_cert = twisted_ssl.PrivateCertificate.loadPEM(client_keypair)

    hostname = parameters.host
    if not isinstance(hostname, six.text_type):
        # Twisted requires the hostname as decoded text, which it isn't in Python 2
        # Decode with the system encoding since this came from the config file. Die,
        # Python 2, die.
        hostname = hostname.decode(locale.getdefaultlocale()[1])
    try:
        context_factory = twisted_ssl.optionsForClientTLS(
            hostname,
            trustRoot=ca_cert or twisted_ssl.platformTrust(),
            clientCertificate=client_cert,
            extraCertificateOptions={"raiseMinimumTo": twisted_ssl.TLSVersion.TLSv1_2},
        )
    except AttributeError:
        # Twisted 12.2 path for EL7 :(
        context_factory = twisted_ssl.CertificateOptions(
            certificate=client_cert.original,
            privateKey=client_cert.privateKey.original,
            caCerts=[ca_cert.original] or twisted_ssl.platformTrust(),
            verify=True,
            requireCertificate=True,
            verifyOnce=False,
            enableSessions=False,
        )

    return context_factory
