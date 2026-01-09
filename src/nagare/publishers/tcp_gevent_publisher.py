# --
# Copyright (c) 2014-2025 Net-ng.
# All rights reserved.
#
# This software is licensed under the BSD License, as described in
# the file LICENSE.txt, which you should have received as part of
# this distribution.
# --

"""Gevent REST API server."""

import os
from functools import partial

from gevent import monkey, server
from gevent import socket as gsocket

from nagare.server import publisher


class Publisher(publisher.Publisher, server.StreamServer):
    """TCP server."""

    CONFIG_SPEC = publisher.Publisher.CONFIG_SPEC | {
        'socket': 'string(default=None)',  # Unix socket to listen on
        'mode': 'integer(default=384)',  # RW mode of the unix socket
        'host': 'string(default="127.0.0.1")',  # TCP host to listen on
        'port': 'integer(default=20000)',  # TCP port to listen on
        'backlog': 'integer(default=256)',  # Max nb of waiting requests,
        'patch_all': 'boolean(default=True)',
        'msg_end': 'string(default=None)',
        'msg_max_len': 'integer(default=1024)',
    }

    def __init__(self, name, dist, patch_all, msg_end, msg_max_len, **config):
        if patch_all:
            monkey.patch_all()  # Monkey patch the Python standard library

        super().__init__(name, dist, patch_all=patch_all, msg_end=msg_end, msg_max_len=msg_max_len, **config)

        self.msg_end = (msg_end or '\n').encode('ascii')
        self.msg_max_len = msg_max_len

    @property
    def endpoint(self):
        socket = self.plugin_config['socket']
        if socket:
            endpoint = f'unix:{socket} -> '
        else:
            endpoint = 'tcp://{}:{}'.format(self.plugin_config['host'], self.plugin_config['port'])

        return not socket, endpoint

    def generate_banner(self):
        return super().generate_banner() + ' on ' + self.endpoint[1]

    @staticmethod
    def recv_msg(sock, delimiter, max_len):
        len_received = 0
        chunks = []

        while True:
            data = sock.recv(max_len)
            if not data:
                break

            while data:
                chunk, d, data = data.partition(delimiter)
                len_received += len(chunk)

                if len_received < max_len:
                    chunks.append(chunk)
                    if d:
                        yield b''.join(chunks)

                if d:
                    len_received = 0
                    chunks = []

    def handle(self, app, services, sock, client):
        self.start_handle_request(app, services, sock=sock, client=client, msg_type='open', msg=None)

        for msg in self.recv_msg(sock, self.msg_end, self.msg_max_len):
            response = self.start_handle_request(app, services, sock=sock, client=client, msg_type='receive', msg=msg)
            if response is not None:
                sock.send(response)

        self.start_handle_request(app, services, sock=sock, client=client, msg_type='close', msg=None)

    def _serve(self, app, host, port, socket, mode, backlog, services_service, **config):
        if socket:
            # Create a unix socket
            listener = gsocket.socket(gsocket.AF_UNIX, gsocket.SOCK_STREAM)
            if os.path.exists(socket):
                os.remove(socket)
            listener.bind(socket)
            listener.listen(backlog)
            os.chmod(socket, mode)
            backlog = None
        else:
            # Create a TCP socket
            listener = (host, port)

        server.StreamServer.__init__(self, listener, partial(self.handle, app, services_service), backlog)

        services_service(super()._serve, app)

        try:
            self.serve_forever()
        except KeyboardInterrupt:
            pass
