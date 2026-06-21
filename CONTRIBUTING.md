# Contributing

Runlet is pre-release. APIs, module names, and behavior may change while the
core model settles.

## Build

Run the full verification suite before opening a pull request:

```bash
./gradlew check
```

This runs compilation, tests, and ktlint.

Useful local tasks:

```bash
./gradlew test
./gradlew ktlintCheck
./gradlew ktlintFormat
./gradlew publishToMavenLocal
```

## Project Layout

- `runlet-core`: core API, DSL, runtime, and blocking adapters.
- `runlet-connector-file`: file source, checkpoint store, and chunk-file sink.
- `runlet-connector-jackson`: Jackson JSON Lines helpers.
- `runlet-adapter-spring`: Spring Framework lifecycle adapter.
- `runlet-spring-boot-autoconfigure`: Spring Boot autoconfiguration.
- `runlet-spring-boot-starter`: convenience starter dependency.

Keep optional integrations in separate modules. `runlet-core` should stay small
and free of framework, serializer, database, and transport dependencies.

## Code Style

- Kotlin code is formatted with ktlint.
- Prefer small, explicit APIs over broad abstractions.
- Keep classes in focused files and packages.
- Add tests for public behavior, failure handling, and lifecycle semantics.
- Do not introduce new runtime dependencies in `runlet-core` unless they are
  fundamental to the core execution model.

## Pull Requests

A good pull request should include:

- a short description of the behavior change
- tests for new or changed behavior
- documentation updates when user-facing APIs change
- a passing `./gradlew check`

## License

By contributing, you agree that your contribution is licensed under the Apache
License, Version 2.0.
