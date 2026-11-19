# library

Resource libraries for the Graphene Go SDK. They turn the native specs of
external systems into ordinary durable Graphene resources — the same handles,
ownership, lifetime, observability and recovery as the built-in kinds.

| Module | Purpose |
|---|---|
| `docker/` | install Docker on an agent; containers, networks and volumes |
| `k8s/` | a generic Kubernetes/Crossplane resource with a native Go object type |
| `git/` | checkout and Git actions on an agent |
| `file/` | a file from inline bytes, a secret or an artifact |
| `host/` | host settings as records: sysctl, sysfs, directories, systemd units; removal restores the machine |

Usage examples and when to reach for each library are in the
[Graphene docs](https://graphene-ci.github.io/docs/libraries).

## Install

Releases: [github.com/graphene-ci/library/releases](https://github.com/graphene-ci/library/releases).

Each directory is a separate Go module, versioned with a module-prefixed tag
(`docker/vX.Y.Z`, `k8s/vX.Y.Z`, …). Add the ones you use:

```bash
go get github.com/graphene-ci/library/docker@v0.2.0
go get github.com/graphene-ci/library/k8s@v0.2.0
go get github.com/graphene-ci/library/git@v0.1.0
go get github.com/graphene-ci/library/file@v0.1.1
go get github.com/graphene-ci/library/host@v0.1.0
```

## Проверка

Для изменений совместно с ещё не опубликованным SDK нужен соседний checkout
`../pipeline`. Подготовка создаёт игнорируемый `go.work`; инструменты прибитых
версий устанавливаются только в `bin/`.

```bash
make configure
make test
make lint
```

`docker/dockertest`, `k8s/k8stest`, `file/filetest` и `host/hosttest` — адаптеры локальной модели пайплайна.
Их wire-типы общие с production-библиотеками, регистрация изолирована между
подготовками. Дополнительно тесты исполняют настоящие entity workflows с
подменой внешних Docker/Kubernetes операций. Пользовательское руководство:
[локальные тесты](https://graphene-ci.github.io/docs/sdk/testing).

Проверка отдельного модуля после подготовки:

```bash
go -C docker test ./...
go -C file  test ./...
go -C git   test ./...
go -C host  test ./...
go -C k8s   test ./...
```

`host` меняет настоящую машину, поэтому его интеграционные тесты идут от root
в одноразовом привилегированном контейнере с systemd (нужен docker). Из sysctl
они трогают только ключи `net.*`, изолированные network namespace контейнера:

```bash
make it-host
```

## Release

Each module releases on its own module-prefixed semver tag (`docker/v0.1.0`);
the release workflow creates the matching GitHub Release page, and the Go proxy
serves the version.
