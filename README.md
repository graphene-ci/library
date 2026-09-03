# library

Ресурсные библиотеки для Go SDK Graphene. Они переводят нативные спецификации
внешних систем в обычные durable-ресурсы Graphene с теми же handles, владением,
lifetime, наблюдаемостью и восстановлением, что у встроенных kinds.

| Модуль | Назначение |
|---|---|
| `docker/` | установка Docker на агент, containers, networks и volumes |
| `k8s/` | generic Kubernetes/Crossplane-ресурс с нативным Go-типом объекта |
| `git/` | checkout и Git-действия на агенте |
| `file/` | файл из inline bytes, secret или artifact |

Пользовательские примеры и правила выбора библиотеки описаны в
[документации Graphene](https://graphene-ci.github.io/docs/libraries).

Каждый каталог — самостоятельный Go-модуль. Текущая проверка выполняется по
модулям:

```bash
go -C docker test ./...
go -C file test ./...
go -C git test ./...
go -C k8s test ./...
```
