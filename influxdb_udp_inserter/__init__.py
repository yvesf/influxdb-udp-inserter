try:
    import struct
except ImportError:
    import ustruct as struct

try:
    import socket
except ImportError:
    import usocket as socket


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

try:
    import hashlib
except ImportError:
    import uhashlib as hashlib


class SerializerFactory:
    def __init__(self):
        self.message_formats = {}

    def add_message_format(self, description):
        if not 'identifier' in description:
            raise Exception('Missing required option "identifier"')
        identifier = description['identifier']
        if not isinstance(identifier, bytes):
            identifier = bytes(identifier)
        if identifier in self.message_formats:
            raise Exception('There is already a message defined with identifier', description['identifier'])
        self.message_formats[identifier] = description

    def get_serializer(self, identifier):
        if not isinstance(identifier, bytes):
            identifier = bytes(identifier)
        if identifier in self.message_formats:
            return MessageSerializer.from_config(self.message_formats[identifier])
        else:
            raise Exception('No message format with identifier', identifier)


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
    def from_config(config):
        identifier = bytes(config['identifier'])
        secret = bytes(config['secret'])
        database = config['database']
        fields = list(MessageSerializer._make_fields(config['fields']))
        return MessageSerializer(identifier, database, secret, fields)

    def __init__(self, identifier, database, secret, fields):
        self.identifier = identifier
        self.database = database
        self.secret = secret
        self.fields = fields
        self.size = sum(map(lambda f: sum(v[1].size for v in f[1:]), self.fields))

    def serialize(self, data: dict):
        if len(data) != len(self.fields):
            raise Exception()
        if set(data.keys()) != set(map(lambda v: v[0], self.fields)):
            raise Exception("Data does not match schema")

        payload = bytes()

        for config_name, *config_values in self.fields:
            data_values = data[config_name]
            if set(data_values.keys()) != set(map(lambda kv: kv[0], config_values)):
                raise Exception("inconsistent data for ", config_name, data_values.keys(), dict(config_values).keys())

            for config_sub_name, config_struct in config_values:
                payload += config_struct.pack(data_values[config_sub_name])

        buf = bytes(self.identifier[0:3]) + payload

        hash = hashlib.sha256(buf + self.secret).digest()
        buf += hash[0:6]

        return buf

    def deserialize(self, data):
        if len(data) < 3 + 6:
            raise Exception('Message of wrong size', len(data))

        hash = hashlib.sha256(data[:-6] + self.secret).digest()
        if hash[0:6] != data[-6:]:
            raise Exception("Failed to authenticate message")

        payload = data[3:-6]
        if len(payload) != self.size:
            raise Exception('Message of wrong payload size', len(payload))

        result = []
        i = 0
        for config_name, *config_values in self.fields:
            result_field = {}

            for config_sub_name, config_struct in config_values:
                window = payload[i:i + config_struct.size]
                result_field[config_sub_name] = config_struct.unpack(window)
                i += config_struct.size

            result += [(config_name, result_field)]

        return dict(result)


def send(host: str, port: int, serializer: MessageSerializer, data: dict):
    serialized_data = serializer.serialize(data)

    sockaddr = socket.getaddrinfo(host, port)[0][-1]
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect(sockaddr)
        s.send(serialized_data)
    finally:
        s.close()
