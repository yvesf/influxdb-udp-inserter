from influxdb_udp_inserter import SerializerFactory

def test():
    try:
        import os
    except ImportError:
        import uos as os
    try:
        import json
    except ImportError:
        import ujson as json

    with open('influxdb_udp_inserter/sample_message_format.js') as fp:
        message_format = json.load(fp)

    factory = SerializerFactory()
    factory.add_message_format(message_format)

    identifier = bytes((0x01, 0x01, 0x01))
    fake_data = {}
    for field_name, *values in factory.get_serializer(identifier).fields:
        fake_data[field_name] = {}
        for name, value in values:
            fake_data[field_name][name] = sum(map(ord, field_name+name)) % 0xff

    print(fake_data)
    serialized = factory.get_serializer(identifier).serialize(fake_data)
    print(serialized)

    print("{}byte => 360 * 24 * 12 => {}Mb".format(len(serialized), (len(serialized) * 360 * 24 * 12) / 1024 / 1024))

    result = factory.get_serializer(identifier).deserialize(serialized)
    print(result)

    if fake_data != result:
            raise Exception()
    else:
        print("Input/Output verified correctly")


if 1==1:
    test()
