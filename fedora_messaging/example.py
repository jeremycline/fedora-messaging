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
"""Example consumers that can be used when starting out with the library to test."""
from __future__ import print_function

import six
import time

from . import exceptions


def printer(message):
    """
    A simple callback that prints the message to standard output.

    Usage: ``fedora-messaging consume --callback="fedora_messaging.example:printer"``

    Args:
        message (fedora_messaging.api.Message): The message that was received.
    """
    print(six.text_type(message))


def bad_consumer(message):
    """ A consumer that misbehaves"""
    time.sleep(30)
    print(six.text_type(message))


def exit(message):
    raise exceptions.HaltConsumer(exit_code=0)
