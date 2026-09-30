# SBE serde for Kafbat UI

A [Kafbat UI](https://github.com/kafbat/kafka-ui) plugin that shows [Simple Binary Encoding](https://github.com/aeron-io/simple-binary-encoding)
(SBE) messages as JSON. It decodes from the SBE XML schema at runtime, so it needs no generated codecs and
works with any schema. To pick up schema changes, update the XML; the plugin doesn't need rebuilding.

It handles everything in SBE: composites, enums, bitsets, repeating groups, var-data, optional and constant
fields, custom message headers, and messages written with an older version of the schema. Decoding only
(messages can't be produced from the UI).

This is an independent plugin, not affiliated with or endorsed by the Kafbat project.

## Build

```sh
./gradlew build
```

This produces `build/libs/ui-serde-sbe-<version>-jar-with-dependencies.jar`. Building needs JDK 25; the plugin runs
on Java 17 or later, so it works with every Kafbat UI 1.x (1.0 runs on Java 17, 1.1 to 1.4 on 21, 1.5 onwards on 25).

### End-to-end test

```sh
./gradlew e2eTest    # needs Docker
```

Starts Kafka and the published Kafbat UI image with the plugin mounted (`e2e/compose.yaml`), produces SBE
messages, and checks them through Kafbat UI's REST API. The test report is at
`build/reports/tests/e2eTest/index.html`.

To see the decoded messages in the UI, keep the environment running after the tests:

```sh
./gradlew e2eTest -Pe2eKeep   # then open http://localhost:18080 (topics sbe-*)
./gradlew e2eDown             # when finished
```

Kafbat UI refreshes its topic list every ~30 seconds, so new topics can take a moment to appear. The ports can be changed with
`-Pe2eUiPort=… -Pe2eKafkaPort=…`, and the image with `E2E_KAFKA_UI_IMAGE`.

## Configure

```yaml
kafka:
  clusters:
    - name: local
      serde:
        - name: SBE
          className: io.github.codebrazier.kafbat.sbe.SbeSerde
          filePath: /plugins/ui-serde-sbe-0.1.0-jar-with-dependencies.jar
          topicValuesPattern: "trades.*"
          properties:
            schemaFiles: /schemas/trades.xml
            decimalConventions: bigDecimal
```

Always set `topicValuesPattern` (and/or `topicKeysPattern`): a serde without a pattern is offered for every
topic.

| Property | Default | |
|---|---|---|
| `schemaFiles` | required | One or more schema XML files (list, or comma-separated). `xi:include` is supported. Each message is matched to a schema by the schema id in its header. |
| `messageOffset` | `0` | Bytes to skip before the first message header, for records with a prefix. |
| `multipleMessagesPerRecord` | `false` | Decode consecutive messages to the end of the record, shown as a JSON array. |
| `decimalConventions` | `fix` | How decimals are encoded (below). Empty to show their raw fields. |
| `maxDecimalExponent` | `1000` | Decimals with an exponent beyond ± this are shown as their raw fields (below). |

### Schemas that share an id

Schema ids within one serde must be unique, since the header's schema id is how a message's schema is found.
If several schemas use the same id, register the serde once per schema, each restricted to its own topics:

```yaml
      serde:
        - name: SBE trades
          className: io.github.codebrazier.kafbat.sbe.SbeSerde
          filePath: /plugins/ui-serde-sbe.jar
          topicValuesPattern: "trades.*"
          properties:
            schemaFiles: /schemas/trades.xml
        - name: SBE orders
          className: io.github.codebrazier.kafbat.sbe.SbeSerde
          filePath: /plugins/ui-serde-sbe.jar
          topicValuesPattern: "orders.*"
          properties:
            schemaFiles: /schemas/orders.xml
```

### Decimals

SBE has no standard decimal type, so decimals are composites of integers. A convention recognises one encoding
by shape and shows the value as a plain decimal string (a string, so the UI doesn't lose precision).

| Convention | Matches | Value |
|---|---|---|
| `fix` | a composite of exactly `mantissa`, `exponent` | `mantissa × 10^exponent`; null if the mantissa is null |
| `bigDecimal` | a composite of exactly `length`, `mantissa` (byte array), `exponent`; or sibling fields `xLength`, `xMantissa`, `xExponent` (shown as `x`) | Java `BigDecimal`'s representation: the first `length` mantissa bytes are the unscaled value (as from `BigInteger.toByteArray()`) and `exponent` is the scale; null if `length` is 0 |

A decimal whose exponent is beyond ±`maxDecimalExponent` is shown as its raw fields instead. Written out in full
it would be mostly zeros, and a corrupt exponent could need gigabytes. Real encodings stay far below the default.

## Output

Each message becomes a JSON object whose `$message` field is the message name, followed by its fields in
schema order. Enums show their names (or the raw value if it isn't in the schema), bitsets show the names of
the set choices, and var-data with a `characterEncoding` shows as text (otherwise hex).

The UI also shows `sbeMessage`, `sbeSchemaId`, `sbeTemplateId` and `sbeVersion` for each record. A record that
can't be decoded (unknown schema or template id, or truncated) is shown as hex with the reason in `sbeError`,
rather than failing the page.

## Notes

- Agrona's buffers need `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`, which Kafbat UI isn't started
  with, so the plugin decodes through its own bounds-checked buffer instead. `./gradlew check` runs a test in a
  JVM without that flag to keep it that way.
- The tests encode messages with codecs generated from `src/test/resources/sbe/trading.xml`.
