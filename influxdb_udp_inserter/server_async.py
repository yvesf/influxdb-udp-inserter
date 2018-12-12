from . import SerializerFactory

from influxdb import line_protocol

import logging
import json
import asyncio
import aiohttp


class UdpInserterProtocol(asyncio.DatagramProtocol):
    def __init__(self, factory: SerializerFactory, influx_url: str):
        self._factory = factory
        self._influx_url = influx_url
        self._transport: asyncio.Transport = None
        self._max_delta_t = 10
        self._known_nonces = {}  # key: timestamp, values: set( (identifier, nonce) )

    def connection_made(self, transport):
        self._transport = transport

    def cleanup_known_nonces(self):
        now = self._factory.timesource.unix_time_sec()
        delete = []
        for key in self._known_nonces.keys():
            if key < now - self._max_delta_t:
                delete.append(key)
        for key in delete:
            del self._known_nonces[key]

    def datagram_received(self, raw_data, addr):
        logging.info('Received %s bytes: %r(...) from %s', len(raw_data), raw_data[0:3], addr)
        if len(raw_data) < 7: return

        identifier = raw_data[0:3]
        serializer = self._factory.get_serializer(identifier)

        mesg, fields = serializer.deserialize(raw_data, self._max_delta_t)

        # Verify nonce is not known for that timestamp
        if mesg.timestamp in self._known_nonces.keys() and \
                mesg.nonce in self._known_nonces[mesg.timestamp]:
            raise Exception('Possible replay attack: Nonce {} already knwon for timestamp {}'.format(
                mesg.nonce, mesg.timestamp))
        else:
            if not mesg.timestamp in self._known_nonces.keys():
                self._known_nonces[mesg.timestamp] = set()
            self._known_nonces[mesg.timestamp].add(mesg.nonce)

        influxdb_points = []
        for key, value in fields.items():
            influxdb_points.append({
                'measurement': key,
                'tags': {},
                'fields': dict(value)
            })

        post_data = line_protocol.make_lines({'points': influxdb_points}).encode()

        asyncio.ensure_future(send(self._influx_url + '?db=' + serializer.database, post_data))

        self.cleanup_known_nonces()


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
