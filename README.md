# Build

```
make
```

# Install

```
sudo make install
```

# Run

```
observation-encoder -config /path/to/config/file
```

# Sample config file

```toml
address = "127.0.0.1"
port = "10000"

[cert]
interval = 100
cert_dir = "/path/to/certs/dir"

[api]
active = true
address = "127.0.0.1"
port = "10001"

[nats]
watch_subject = "test.observations.*.>"
out_subject = "test.observations.down"
url = "nats://127.0.0.1:4222"
```
