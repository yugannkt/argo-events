Kafka is a widely used event streaming platform. We recommend using Kafka if
you have a lot of events and want to horizontally scale your Sensors. If you
are looking to get started quickly with Argo Events we recommend using
Jetstream instead.

When using a Kafka EventBus you must already have a Kafka cluster set up and
topics created (unless you have auto create enabled, see [topics](#topics)
below).

## Example

### Basic Configuration with Direct URL

```yaml
apiVersion: argoproj.io/v1alpha1
kind: EventBus
metadata:
  name: default
spec:
  kafka:
    url: kafka:9092 # must be managed independently
    topic: "example" # optional
```

### Using Secret Reference for URL

For environments where connection details are managed via Kubernetes Secrets
(e.g., when using HashiCorp Vault, External Secrets Operator, or other secret
management systems), you can reference a secret instead of hardcoding the URL:

```yaml
apiVersion: argoproj.io/v1alpha1
kind: EventBus
metadata:
  name: default
spec:
  kafka:
    urlSecret:
      name: kafka-secret
      key: url
    topic: "example" # optional
```

With a corresponding Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-secret
type: Opaque
stringData:
  url: "broker1:9092,broker2:9092,broker3:9092"
```

### Complete Example with TLS and SASL Authentication

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: kafka-credentials
  namespace: argo-events
type: Opaque
stringData:
  bootstrap-servers: "kafka-0.kafka:9093,kafka-1.kafka:9093"
  username: "argo-events-user"
  password: "your-secure-password"
---
apiVersion: v1
kind: Secret
metadata:
  name: kafka-tls
  namespace: argo-events
type: Opaque
data:
  ca.crt: <base64-encoded-ca-certificate>
  client.crt: <base64-encoded-client-certificate>
  client.key: <base64-encoded-client-key>
---
apiVersion: argoproj.io/v1alpha1
kind: EventBus
metadata:
  name: default
  namespace: argo-events
spec:
  kafka:
    urlSecret:
      name: kafka-credentials
      key: bootstrap-servers
    topic: "argo-events"
    tls:
      caCertSecret:
        name: kafka-tls
        key: ca.crt
      clientCertSecret:
        name: kafka-tls
        key: client.crt
      clientKeySecret:
        name: kafka-tls
        key: client.key
    sasl:
      mechanism: SCRAM-SHA-512
      userSecret:
        name: kafka-credentials
        key: username
      passwordSecret:
        name: kafka-credentials
        key: password
```

### Integration with External Secrets Operator

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: kafka-credentials
  namespace: argo-events
spec:
  refreshInterval: 1h
  secretStoreRef:
    name: vault-backend
    kind: SecretStore
  target:
    name: kafka-credentials
  data:
    - secretKey: bootstrap-servers
      remoteRef:
        key: secret/data/kafka
        property: bootstrap_servers
---
apiVersion: argoproj.io/v1alpha1
kind: EventBus
metadata:
  name: default
  namespace: argo-events
spec:
  kafka:
    urlSecret:
      name: kafka-credentials
      key: bootstrap-servers
```

See [here](../APIs.md#argoproj.io/v1alpha1.KafkaBus)
for the full specification.

## Properties

### url

Comma separated list of kafka broker urls, the kafka broker must be managed
independently of Argo Events. Mutually exclusive with `urlSecret`.

Example:
```yaml
url: "broker1:9092,broker2:9092,broker3:9092"
```

### urlSecret

A reference to a Kubernetes Secret containing the Kafka URL. This allows
the Kafka connection details to be managed via Kubernetes Secrets, which is
useful for integrating with secret management systems like Vault or External
Secrets. The secret value should contain a comma separated list of kafka
broker urls.

```yaml
urlSecret:
  name: kafka-secret
  key: url
```

**Validation Rules:**
- Mutually exclusive with `url`
- Either `url` or `urlSecret` must be specified, but not both
- Specifying both will result in a validation error: `url and urlSecret are mutually exclusive`
- Specifying neither will result in a validation error: `either url or urlSecret must be specified`

**Benefits of using `urlSecret`:**
- Centralizes connection details in Kubernetes Secrets
- Enables integration with external secret management (Vault, AWS Secrets Manager, etc.)
- Reduces duplication across manifests
- Simplifies credential rotation
- Follows Kubernetes best practices for sensitive configuration

### topic

The topic name, defaults to `{namespace-name}-{eventbus-name}`. Two additional
topics per Sensor are also required, see [topics](#topics) below for more
information.

### version

Kafka version, we recommend not manually setting this field in most
circumstances. Defaults to the oldest supported stable version.

### tls

Enables TLS on the kafka connection.

```
tls:
  caCertSecret:
    name: my-secret
    key: ca-cert-key
  clientCertSecret:
    name: my-secret
    key: client-cert-key
  clientKeySecret:
    name: my-secret
    key: client-key-key
```

### sasl

Enables SASL authentication on the kafka connection.

```
sasl:
  mechanism: PLAIN
  passwordSecret:
    key: password
    name: my-user
  userSecret:
    key: user
    name: my-user
```

### consumerGroup.groupName

Consumer group name, defaults to `{namespace-name}-{sensor-name}`.

### consumerGroup.rebalanceStrategy

The kafka rebalance strategy, can be one of: sticky, roundrobin, range.
Defaults to range.

### consumerGroup.startOldest

When starting up a new group do we want to start from the oldest event
(true) or the newest event (false). Defaults to false

### partitioner

Producer partitioning strategy. Supported values: `random`, `hash`, `roundrobin`, `manual`. Defaults to `random`.

## Security

You can enable TLS or SASL authentication, see above for configuration
details. You must enable these features in your Kafka Cluster and make
the certificates/credentials available in a Kubernetes secret.

### Secret Management Best Practices

When configuring Kafka EventBus, we recommend using secret references for all
sensitive configuration:

| Configuration | Secret Field | Description |
|---------------|--------------|-------------|
| Broker URLs | `urlSecret` | Kafka bootstrap servers |
| TLS CA Cert | `tls.caCertSecret` | CA certificate for TLS |
| TLS Client Cert | `tls.clientCertSecret` | Client certificate for mTLS |
| TLS Client Key | `tls.clientKeySecret` | Client private key for mTLS |
| SASL Username | `sasl.userSecret` | SASL authentication username |
| SASL Password | `sasl.passwordSecret` | SASL authentication password |

### How Secrets Are Mounted

Secrets referenced in the EventBus configuration are automatically mounted
into the EventSource and Sensor pods at runtime:

1. **Mount Path**: Secrets are mounted at `/argo-events/secrets/{secret-name}/{key}`
2. **Automatic Discovery**: The controller uses reflection to find all
   `SecretKeySelector` fields in your configuration
3. **Runtime Resolution**: Values are read from the mounted files when
   establishing Kafka connections

This approach:
- Enables automatic secret rotation (pods see updated values when secrets change)
- Follows Kubernetes-native patterns for secret management
- Works seamlessly with external secret operators

## Topics

The Kafka EventBus requires one event topic and two additional topics (trigger
and action) per Sensor. These topics will not be created automatically unless
the Kafka `auto.create.topics.enable` cluster configuration is set to true,
otherwise it is your responsibility to create these topics. If a topic does
not exist and cannot be automatically created, the EventSource and/or Sensor
will exit with an error.

If you want to take advantage of the horizontal scaling enabled by the Kafka
EventBus be sure to create topics with more than one partition.

By default the topics are named as follows.

| topic   | name                                                |
| ------- | --------------------------------------------------- |
| event   | `{namespace}-{eventbus-name}`                       |
| trigger | `{namespace}-{eventbus-name}-{sensor-name}-trigger` |
| action  | `{namespace}-{eventbus-name}-{sensor-name}-action`  |

If a topic name is specified in the EventBus specification, then the topics are
named as follows.

| topic   | name                                       |
| ------- | ------------------------------------------ |
| event   | `{spec.kafka.topic}`                       |
| trigger | `{spec.kafka.topic}-{sensor-name}-trigger` |
| action  | `{spec.kafka.topic}-{sensor-name}-action`  |

## Horizontal Scaling and Leader Election

Sensors that use a Kafka EventBus can scale horizontally. Specifying replicas
greater than one will result in all Sensor pods actively processing events.
However, an EventSource that uses a Kafka EventBus cannot necessarily be
horizontally scaled in an active-active manner, see [EventSource HA](../eventsources/ha.md)
for more details. In an active-passive scenario a [Kubernetes leader election](../eventsources/ha.md#kubernetes-leader-election)
is used.
