# kafka-riak-commitlog

The post-commit hook for producing Riak commits to a Kafka topic.

## Message format

```json
{
  "bucket": "RIAK BUCKET",
  "key": "RIAK OBJECT KEY",
  "value": "RIAK OBJECT VALUE"
}
```

### Example

```json
{
  "bucket": "gateways",
  "key": "MAoc7xHDGk0zDvZMFqv7nRfw2kO",
  "value": {
    "created_at": "2016-03-31T20:58:43Z",
    "state": "retained",
    "account_key": "UOWPC9x2egNEFRQHshOgTxQnr4C",
    "description": null,
    "updated_at": "2016-03-31T20:58:43Z",
    "gateway_type": "test",
    "transaction_fee_amount": null,
    "_type": "TestGateway"
  }
}
```

## Developer Installation

### Compile and install the post-commit hook

```
$ make
$ make install
```

### Check that the code is in place

```
$ make ls-install
```

### Configure riak to call the post-commit hook

```
$ ./post-commit-hooks add
```

### Restart local riak nodes

```
$ ./post-commit-hooks rolling-restart-riak
```

Done! At this point any commits to your local core or id riak nodes should be writing their object details to a Kafka topic called `commitlog`.
