# UDP Wire protocol for influxdb

Key points:

- UDP datagramm based. Inherits non-reliable transport (no acks) and unordered.
- No transport encryption
- No transport compression
- Messages are authenticated by sha256[0:6] of data + secret value + 16-bit nonce + 64-bit timestamp.
- Replay is prevented by 2^16 nonce values per timestamp-second. Messages are only valid in timestamp window.
- All client-relevant code is compatible with micropython
