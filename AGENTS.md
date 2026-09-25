# AGENTS.md

Notes for anyone (human or agent) picking up work here. For what the plugin does and how to configure it,
see `README.md`.

## Commands

```sh
./gradlew build          # compile, both test tasks, plugin jar
./gradlew test           # main tests (JVM has --add-exports, for the generated test encoders)
./gradlew noUnsafeTest   # tests tagged "no-unsafe", in a JVM like kafka-ui's (no --add-exports)
./gradlew shadowJar      # build/libs/ui-serde-sbe-<version>-jar-with-dependencies.jar
./gradlew e2eTest        # plugin inside the real Kafbat UI image, via Docker (not part of `check`)
```

- JDK 25 is required: `io.kafbat.ui:serde-api:1.1.0` is published for Java 25 only (kafka-ui runs on 25).
- serde-api depends on Confluent's `kafka-clients` `x.y.z-ccs`, so `build.gradle` adds Confluent's Maven
  repo, limited to the `org.apache.kafka` group.
- `compileJava` uses `-Xlint:all -Werror`, so deprecated Jackson APIs fail the build (e.g. use
  `ObjectNode.properties()`, not `fields()`). Test code, including the generated codecs, isn't held to that.
- Make variables `final` wherever possible, in main, test and e2e code: locals, parameters, catch parameters and
  enhanced-`for` variables. Leave a local non-final only when it's reassigned. Lambda parameters and record
  components are exempt. Nothing enforces this, so keep to it by hand.

## Layout

All in `src/main/java/io/kafbat/ui/serde/sbe/`:

| Class | Role |
|---|---|
| `SbeSerde` | The kafka-ui `Serde`: reads properties, turns decode failures into a hex result with `sbeError` |
| `SbeSchemas` | Parses schema XML (XInclude on) into SBE `Ir`, indexed by schema id; rejects duplicate ids and mismatched headers |
| `SbeRecordDecoder` | Reads headers, runs `OtfMessageDecoder` per message, handles offset/multiple messages/trailing bytes |
| `JsonTreeListener` | SBE `TokenListener` that builds a Jackson tree |
| `DecimalConvention` | Post-processing of the tree that turns decimal-shaped objects into plain decimal strings |
| `ByteArrayReadBuffer` | Read-only `DirectBuffer` over `byte[]` without `Unsafe` (see below) |

## Constraints that aren't obvious from the code

- **No Agrona `Unsafe` in the decode path.** Agrona 2.x buffers (`UnsafeBuffer`, `ExpandableArrayBuffer`, …)
  throw unless the JVM has `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`, and kafka-ui's image
  doesn't set it. Decoding must only go through `ByteArrayReadBuffer`. If you touch other Agrona or SBE
  classes at runtime, add coverage to `NoUnsafeDecodingTest`, which runs without the flag and asserts
  Agrona's buffers really are unavailable.
- **`NoUnsafeDecodingTest` uses a hex fixture**, because it can't use the generated encoders. It's the output
  of `TestMessages.order(2)`. If you change `trading.xml` or `TestMessages`, regenerate it: temporarily print
  `HexFormat.of().formatHex(TestMessages.order(2))` from a test in the normal `test` task and paste it in.
- **Naming inside composites.** `OtfMessageDecoder` passes the *enclosing field* token for members of a
  composite, so `JsonTreeListener` tracks `compositeLevel` and takes names from the type tokens there. This
  mirrors SBE's own `uk.co.real_logic.sbe.json.JsonTokenListener`, which is the reference when in doubt
  (sources: `uk.co.real-logic:sbe-tool:<version>:sources`).
- **Decimals are strings on purpose.** The kafka-ui frontend parses JSON into JavaScript numbers, which would
  lose precision. Integers (including uint64 above 2^63, emitted as `BigInteger`) stay numbers.
- **Conventions match by shape, not type name**, so they work for any schema. Add a new one only for an
  encoding that's widely used; something only one schema does belongs in configuration, not in code.
- **`couldBePreferable` stays at the default (true).** kafka-ui offers a serde without a topic pattern for
  every topic, so the README tells users to always set `topicValuesPattern`.
- **Test schema codegen:** `generateTestCodecs` runs `SbeTool` on `src/test/resources/sbe/trading.xml`. SBE
  generates a static `<field>Id()` for every field, so a field named `x` clashes with one named `xId` in the
  same message or group.

## End-to-end tests

`e2eTest` builds the jar, runs `docker compose up --wait` on `e2e/compose.yaml` (Kafka plus
`ghcr.io/kafbat/kafka-ui:latest`), runs `src/e2e/java`, then always runs `e2eDown`. That task saves Kafbat UI's
log to `build/e2e/kafka-ui.log` before tearing the environment down; look there first when an e2e test fails.

- The serdes are configured with **environment variables on purpose**: that checks Spring's relaxed binding
  of `..._PROPERTIES_SCHEMAFILES` onto `schemaFiles`. Keep at least one property configured that way.
- Each topic is decoded by the serde whose `TOPICVALUESPATTERN` matches it. `sbe-quotes` uses `quotes.xml`,
  which shares schema id 42 with `trading.xml`, to check separate serde instances keep them apart.
- The e2e source set reuses the test output (the generated codecs and `TestMessages`), so e2e messages are
  the same ones the unit tests check.
- Default host ports are 18080 (UI) and 19092 (Kafka), to avoid clashing with a local Kafka.
- `-Pe2eKeep` skips `e2eDown` so the environment and its messages stay up for browsing. The next `e2eTest`
  recreates the containers (`--force-recreate`), so leftover state doesn't leak between runs.
- Kafbat UI's topic list comes from statistics it refreshes periodically, so a brand-new topic can be missing
  from `/topics` for a few seconds. Reading messages works straight away, which is why the tests do that.
- The plugin is compiled for Java 25. If `E2E_KAFKA_UI_IMAGE` points to an older Kafbat UI on an older JDK,
  loading will fail with `UnsupportedClassVersionError`.

## Test data

Test schemas and messages are written for this repo. Don't add schemas or captured messages from other
projects: they may not be licensed for redistribution, and small purpose-built schemas make clearer tests.

To try the plugin against another schema, point a serde at it without committing it: either
`./gradlew e2eTest -Pe2eKeep` with an extra volume and serde in a local copy of `e2e/compose.yaml`, or a
throwaway class with the shadow jar, `serde-api` and that schema's generated codecs on the classpath (encoding
with Agrona needs `--add-exports java.base/jdk.internal.misc=ALL-UNNAMED`).

## Next steps

- CI isn't set up. It would run `./gradlew build` and, as a separate job with Docker, `./gradlew e2eTest`.
- Producing messages (`canSerialize`) is deliberately unsupported. Encoding JSON from the `Ir` is possible but
  much more work.
