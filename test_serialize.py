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
        identifier = bytes(message_format['identifier'])

    factory = SerializerFactory()
    factory.add_message_format(message_format)

    # generate some fake data
    fake_data = {}
    for field_name, *values in factory.get_serializer(identifier).fields:
        fake_data[field_name] = {}
        for name, value in values:
            fake_data[field_name][name] = sum(map(ord, field_name+name)) % 0xff

    # serialize data
    print(fake_data)
    message1, serialized = factory.get_serializer(identifier).serialize(fake_data, 123)
    print(serialized)

    print("{}byte => 360 * 24 * 12 => {}Mb".format(len(serialized), (len(serialized) * 360 * 24 * 12) / 1024 / 1024))

    # deserialize
    message2, result = factory.get_serializer(identifier).deserialize(serialized, 2)

    print(result)

    # Compare numbers
    if message2.nonce != 123:
        raise Exception()
    if fake_data != result:
            raise Exception()
    else:
        print("Input/Output verified correctly")


if 1==1:
    test()
