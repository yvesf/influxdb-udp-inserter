# UDP Wire protocol for influxdb

Key points:

- UDP datagramm based
- No ack's, no transport encryption
- No transport compression (there is not a huge gain and lack of support in micropython)
- Messages are authenticated by hash of secret value
- All client-relevant code is compatible with micropython