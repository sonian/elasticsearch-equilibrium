In elasticsearch.yml:

```yaml
cluster.routing.minimumAvailablePercentage: 20.0
```

```
sudo -u nobody bin/plugin -remove elasticsearch-equilibrium

sudo -u nobody bin/plugin -install elasticsearch-equilibrium -url file:///path/to/elasticsearch-equilibrium-0.19.0-SNAPSHOT.jar
```
