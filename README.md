## 0.19.0

In elasticsearch.yml:

```yaml
cluster.routing.minimumAvailablePercentage: 20.0
```

This plugin currently keeps ES from allocating shards to a node with
less than minimumAvailablePercentage percentage available disk space
on the ES data mount points.


## Master

In elasticsearch.yml:

```yaml
sonian.elasticsearch.equilibrium:
  enabled: true
  minimumAvailablePercentage: 20.0
```
