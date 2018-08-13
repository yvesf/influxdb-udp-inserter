from influxdb_udp_inserter import send, MessageSerializer

try:
    import json
except ImportError:
    import ujson as json

with open('influxdb_udp_inserter/sample_message_format.js') as fp:
    serializer = MessageSerializer.from_config(json.load(fp))



fake_data = {}
for field_name, *values in serializer.fields:
    fake_data[field_name] = {}
    for name, value in values:
        fake_data[field_name][name] = sum(map(ord, field_name+name)) % 0xff
send('0.0.0.0', 9999, serializer, fake_data)

print('done')
