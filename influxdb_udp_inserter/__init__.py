try:
    import struct
except ImportError:
    # noinspection PyUnresolvedReferences
    import ustruct as struct

try:
    import socket
except ImportError:
    # noinspection PyUnresolvedReferences
    import usocket as socket

try:
    import time
except ImportError:
    # noinspection PyUnresolvedReferences
    import utime as time

import builtins


def property(**kwargs):
    """Wrapper for micropython which otherwise doesn't have named arguments"""
    return builtins.property('fget' in kwargs and kwargs['fget'] or None,
                             'fset' in kwargs and kwargs['fset'] or None)


class Struct:
    def __init__(self, fmt):
        self.fmt = fmt
        self.size = struct.calcsize(self.fmt)

    def pack(self, value):
        return struct.pack(self.fmt, value)

    def unpack(self, data):
        return struct.unpack(self.fmt, data)[0]


UINT8 = Struct('B')
UINT16 = Struct('H')
UINT32 = Struct('I')
UINT64 = Struct('Q')

try:
    import hashlib
except ImportError:
    # noinspection PyUnresolvedReferences
    import uhashlib as hashlib


class Timesource:
    def unix_time_sec(self):
        return int(time.time())


class SerializerFactory:
    def __init__(self, timesource: Timesource = None):
        self._timesource = timesource or Timesource()
        self._message_formats = {}

    def add_message_format(self, description):
        if 'identifier' not in description:
            raise Exception('Missing required option "identifier"')
        identifier = description['identifier']
        if not isinstance(identifier, bytes):
            identifier = bytes(identifier)
        if identifier in self._message_formats:
            raise Exception('There is already a message defined with identifier', description['identifier'])
        self._message_formats[identifier] = description

    def get_serializer(self, identifier):
        if not isinstance(identifier, bytes):
            identifier = bytes(identifier)
        if identifier in self._message_formats:
            return MessageSerializer.from_config(self._message_formats[identifier], self._timesource)
        else:
            raise Exception('No message format with identifier', identifier)

    def __get_timesource(self):
        return self._timesource

    timesource = property(fget=__get_timesource)


class Message:
    def __init__(self):
        self._identifier = None
        self._nonce = None
        self._timestamp = None
        self._secret = None
        self._payload = None
        self._timestamp = None

    def __get_identifier(self) -> bytes:
        return self._identifier

    def __set_identifer(self, identifer: bytes):
        self._identifier = identifer

    def __get_nonce(self) -> int:
        return self._nonce

    def __set_nonce(self, nonce: int):
        self._nonce = nonce

    def __set_payload(self, payload):
        self._payload = payload

    def __get_payload(self) -> bytes:
        return self._payload

    def __set_timestamp(self, timestamp: int):
        self._timestamp = timestamp

    def __get_timestamp(self) -> int:
        return self._timestamp

    identifier = property(fget=__get_identifier, fset=__set_identifer)
    nonce = property(fget=__get_nonce, fset=__set_nonce)
    payload = property(fget=__get_payload, fset=__set_payload)
    timestamp = property(fget=__get_timestamp, fset=__set_timestamp)


class MessageWriter:
    def __init__(self, secret):
        self._secret = secret

    def to_bytes(self, message):
        if message.identifier is None or message.timestamp is None \
                or message.payload is None or self._secret is None:
            raise Exception('Writer/Message not correctly initialized')
        else:
            data = message.identifier + UINT16.pack(message.nonce) + message.payload
            timestamp = UINT64.pack(message.timestamp)
            message_hash = hashlib.sha256(data + self._secret + timestamp).digest()
            return data + message_hash[0:6]


class MessageSerializer:
    @staticmethod
    def _make_fields(fields):
        for field_name, *values in fields:
            v = []
            for name, value in values:
                if value.lower() == 'uint16':
                    v += [(name, UINT16)]
                elif value.lower() == 'uint32':
                    v += [(name, UINT32)]
                elif value.lower() == 'uint8':
                    v += [(name, UINT8)]
                else:
                    raise Exception('Datatype not supported', value)
            result = [field_name] + v
            yield result

    @staticmethod
    def from_config(config, timesource: Timesource = None):
        identifier = bytes(config['identifier'])
        secret = bytes(config['secret'])
        database = config['database']
        fields = list(MessageSerializer._make_fields(config['fields']))
        return MessageSerializer(identifier, database, secret, fields, timesource)

    def __init__(self, identifier: bytes, database: str, secret, fields, timesource: Timesource = None):
        if timesource is None:
            self._timesource = Timesource()
        else:
            self._timesource = timesource

        self._identifier = identifier
        self._database = database
        self._secret = secret
        self._fields = fields
        self._size = sum(map(lambda f: sum(v[1].size for v in f[1:]), self._fields))

    def __get_fields(self):
        return self._fields

    fields = property(fget=__get_fields)

    def __get_database(self):
        return self._database

    database = property(fget=__get_database)

    def serialize(self, data: dict, nonce: int) -> (Message, bytes):
        if len(data) != len(self._fields):
            raise Exception()

        if set(data.keys()) != set(map(lambda v: v[0], self._fields)):
            raise Exception("Data does not match schema")

        message = Message()
        message.identifier = self._identifier
        message.nonce = nonce

        payload = bytes()
        for config_name, *config_values in self._fields:
            data_values = data[config_name]
            if set(data_values.keys()) != set(map(lambda kv: kv[0], config_values)):
                raise Exception("inconsistent data for ", config_name, data_values.keys(), dict(config_values).keys())

            for config_sub_name, config_struct in config_values:
                payload += config_struct.pack(data_values[config_sub_name])

        message.payload = payload
        message.timestamp = self._timesource.unix_time_sec()

        writer = MessageWriter(self._secret)

        return message, writer.to_bytes(message)

    def deserialize(self, raw_data, max_delta_t) -> (Message, dict):
        message = self.parse_and_verify(raw_data, max_delta_t)

        if len(message.payload) != self._size:
            raise Exception('Message of wrong payload size', len(message.payload))

        result = []
        i = 0
        for config_name, *config_values in self._fields:
            result_field = {}

            for config_sub_name, config_struct in config_values:
                window = message.payload[i:i + config_struct.size]
                result_field[config_sub_name] = config_struct.unpack(window)
                i += config_struct.size

            result += [(config_name, result_field)]

        return message, dict(result)

    def parse_and_verify(self, raw_data, max_delta_t):
        if len(raw_data) < 3 + 6 + 2:  # 24-bit identifier + 64-bit hash + 16-bit nonce + ... payload
            raise Exception('Message of wrong size', len(raw_data))

        timestamp = self._timesource.unix_time_sec()

        in_data, in_message_hash = raw_data[:-6], raw_data[-6:]
        message = Message()
        message.identifier = in_data[0:3]
        message.nonce = UINT16.unpack(in_data[3:5])
        message.payload = raw_data[5:-6]

        for delta_t in range(-1 * max_delta_t, max_delta_t):
            t = UINT64.pack(timestamp + delta_t)
            message._message_hash = hashlib.sha256(in_data + self._secret + t).digest()[0:6]
            if message._message_hash == in_message_hash:
                message.timestamp = timestamp + delta_t
                break
        else:
            raise Exception("Failed to authenticate message", )

        return message


def send(host: str, port: int, serializer: MessageSerializer, data: dict, nonce: int):
    """:param nonce: is truncated to unsigned 16bit"""
    _, serialized_data = serializer.serialize(data, nonce)

    sockaddr = socket.getaddrinfo(host, port)[0][-1]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(sockaddr)
        s.send(serialized_data)
    finally:
        s.close()
