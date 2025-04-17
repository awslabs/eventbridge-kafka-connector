# How to run

Compile the project.

```console
mvn clean package -Drevision=$(git describe --tags --always)
```

Bring up Kafka environment e.g., using the `e2e` example.

```console
docker compose -f docker_compose.yaml down --remove-orphans -v && docker compose -f docker_compose.yaml up
```

Start the payment producer.

```console
java -cp target/payment-app-1.0.0-shaded.jar com.example.payment.PaymentProducer
```

Start the fraud detector.

```console
java -cp target/payment-app-1.0.0-shaded.jar com.example.payment.PaymentFraudDetector
```

Topics are hardcoded to `payments` and `fraud`. To consume from `fraud`:

```console
kafkactl consume -b fraud | jq .
```