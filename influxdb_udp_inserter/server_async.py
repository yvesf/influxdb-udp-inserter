from . import SerializerFactory

from influxdb import line_protocol

import logging
import json
import asyncio
import aiohttp


class UdpInserterProtocol(asyncio.DatagramProtocol):
    def __init__(self, factory: SerializerFactory, influx_url: str):
        self.factory = factory
        self.influx_url = influx_url
        self.transport: asyncio.Transport = None

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        logging.info('Received %s bytes: %r(...) from %s', len(data), data[0:3], addr)
        if len(data) < 7: return

        serializer = self.factory.get_serializer(data[0:3])
        mesg = serializer.deserialize(data)

        influxdb_points = []
        for key, value in mesg.items():
            influxdb_points.append({
                'measurement': key,
                'tags': {},
                'fields': dict(value)
            })

        post_data = line_protocol.make_lines({'points': influxdb_points}).encode()

        asyncio.ensure_future(send(self.influx_url + '?db=' + serializer.database, post_data))


async def send(url, data):
    logging.info("Send data: %r", data)
    async with aiohttp.ClientSession() as client_session:
        async with client_session.post(url,
                                       headers={aiohttp.hdrs.CONTENT_TYPE: 'application/x-www-form-urlencoded'},
                                       data=data) as http_resp:
            body = await http_resp.read()
            logging.info("Received response %s (0..200): %r", http_resp.status, body[0:200])
            http_resp.close()


def create_endpoint(factory: SerializerFactory, influx_url: str, *args, **kwargs):
    loop = asyncio.get_event_loop()
    listen = loop.create_datagram_endpoint(lambda: UdpInserterProtocol(factory, influx_url), *args, **kwargs)
    return listen


class Server:
    def __init__(self, influx_url: str, *args, **kwargs):
        self.influx_url = influx_url
        self.serializer_factory = SerializerFactory()
        self.listen = create_endpoint(self.serializer_factory, self.influx_url, *args, **kwargs)

    def run(self):
        loop = asyncio.get_event_loop()
        transport, protocol = loop.run_until_complete(self.listen)
        try:
            loop.run_forever()
        finally:
            transport.close()
            loop.close()

    def read_message_format_file(self, path):
        config = json.load(open(path))
        self.serializer_factory.add_message_format(config)
