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

```json
{
    "address": "127.0.0.1",
    "port": "10000",
    "cert":
    {
        "interval": 100,
        "cert_dir": "/path/to/certificates/dir"
    },
    "api":
    {
        "active": true,
        "address": "127.0.0.1",
        "port": "10001"
    }
}
```
