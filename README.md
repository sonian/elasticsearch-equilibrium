In elasticsearch.yml:

```yaml
cluster.routing.minimumAvailablePercentage: 20.0
```

This plugin currently keeps ES from allocating shards to a node with
less than minimumAvailablePercentage percentage available disk space
on the ES data mount points.
