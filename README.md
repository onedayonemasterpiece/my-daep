# my-daep

**Distributed Agent Execution Platform** — явно включаемый распределённый режим разработки в OpenCode.

Пользователь работает в существующем GitHub-проекте на DevCoveer и указывает: выполнить задачу распределённо на Kaggle. Основная модель делит работу, отдельные OpenCode в 1–4 notebooks реализуют независимые части. DAEP сохраняет изменения в GitHub, восстанавливает сбои, возвращает результаты координатору для интеграции и сквозных проверок либо передачи в ChatGPT.

## Статус реализации

На 10 сентября 2026 source-runtime реализуется в PR #1 (`feat/my-daep-v1-distributed-runtime`). В ветке уже есть исполняемый Python supervisor/CLI, SQLite lifecycle, Kaggle provider, GitHub App installation-token broker, attempt-scoped worker API, генерируемый Kaggle worker с OpenCode и Git checkpoints/readback, обратная доставка significant events в coordinator session, recovery из подтверждённого checkpoint, один проверяемый final PR path и глобальный OpenCode tool `daep`.

Это **не означает, что live acceptance пройден**. В текущем DevCoveer OpenCode route доступна модель OpenCode, но операторской сессии не выдан shell/bash, поэтому через неё пока нельзя клонировать этот repository, создать venv, запустить pytest/Kaggle CLI или развернуть supervisor. DAEP-specific GitHub App/runtime variables также ещё не настроены. Реальные Kaggle workers, token mint/refresh и двух-worker recovery pilot поэтому не заявляются как проверенные.

## Документы

- **[Итоговый проект v1](docs/distributed-mode.md)** — канонический продуктовый контракт: GitHub, App/credentials, распределённая работа, наблюдение, восстановление и приёмка.
- **[Постановка реализации](docs/implementation-task.md)** — порядок source-реализации, установки и проверяемого coding-пилота.

## Реализованный путь

```text
OpenCode в текущем GitHub worktree
       ↓ global tool ~/.config/opencode/tools/daep.ts
DAEP supervisor + SQLite + GitHub App broker
       ↓ Kaggle API                    ↑ events/commands/heartbeats
1–4 private Kaggle notebooks + OpenCode workers
       ↓ exact attempt branches + checkpoints/readback
GitHub target repo
       ↓ exact commits
OpenCode coordinator integration → daep/result/<job> → checks → one PR
```

`daep install-opencode` устанавливает глобальный OpenCode tool. Он получает repository, текущий commit и session из текущего worktree, требует чистый Git checkpoint и поддерживает `submit`, `status`, `follow`, `resume/attach`, `cancel`, `export`, `finalize`.

Supervisor сохраняет job/task/attempt/effect/event/command state в SQLite. Launch intent фиксируется до Kaggle side effect; timeout запуска остаётся неопределённым и сверяется с конкретным provider identity вместо слепого retry. Worker push считается checkpoint только после независимого `ls-remote` readback; потерянный push ACK не создаёт ложный retry. При потере worker следующая generation стартует от последнего принятого checkpoint, а уже завершённые соседние задачи сохраняются.

## Конфигурация runtime

Минимальные настройки supervisor:

```text
DAEP_CONTROL_TOKEN
DAEP_WORKER_HMAC_SECRET
DAEP_PUBLIC_URL=https://<reachable-supervisor>
DAEP_GITHUB_APP_ID
DAEP_GITHUB_PRIVATE_KEY_FILE=/secure/path/daep.pem
DAEP_GITHUB_INSTALLATION_ID=<optional fixed installation>
DAEP_KAGGLE_USERNAME
DAEP_OPENCODE_AUTH_FILE=/secure/path/opencode-auth.json
DAEP_OPENCODE_SERVER_URL=http://127.0.0.1:<managed-opencode-port>
DAEP_BACKUP_DIR=/external/backup/path
```

Для пилота допустим отдельный PEM key существующей GitHub App dataset-loop, если её installation/permissions реально дают доступ целевому repository. Отдельная DAEP App подключается теми же переменными без изменения кода. Credentials и rollout dataset-loop менять не требуется.

Notebook получает не PEM, а короткоживущий repository-scoped installation token. Broker использует фактический `expires_at`, кэш с refresh-skew и конечный повтор после 401. Git операции worker выполняет через `GIT_ASKPASS`; credentials не помещаются в Git remote URL и не должны попадать в исходники/логи.

## Локальный запуск после provisioning

```bash
python -m venv .venv
. .venv/bin/activate
pip install -e '.[test]'
python -m compileall src tests
pytest
daep install-opencode
daep serve
```

Перед live запуском отдельно проверить `kaggle --version`, GitHub App preflight на выбранном target repository и доступность `DAEP_PUBLIC_URL` из Kaggle. Эти команды в текущей DevCoveer OpenCode-сессии ещё не выполнялись из-за отсутствия shell capability, поэтому README не выдаёт их за пройденные.

## GitHub и доступ

GitHub — обязательный источник кода и внешняя сохранность code checkpoints/результатов. У каждой попытки собственная ветка `daep/work/<job>/<task>/a<generation>`. Интеграция публикуется в `daep/result/<job>`. Finalize сверяет remote SHA integration branch и идемпотентно находит/создаёт единственный итоговый PR.

GitHub App token ограничивается repository и permissions, но не одной веткой. Именование веток и fencing защищают принятие результатов от stale attempts; это не подменяется фиктивной branch-level token isolation.

GitHub не хранит живую очередь, heartbeats, SQLite, build cache и полные session dumps. Они остаются в runtime state и внешнем backup. Никакого нового GitHub runner, webhook или обязательного MCP/OAuth для DAEP v1.

## Границы

Только Kaggle; ручной предел 1–4 и общий максимум четыре; разные проекты через один глобальный вход. Нельзя молча заменить distributed mode локальным кодингом. Другие backend, собственный CI/CD/UI, model gateway, code classification/DLP и миграция dataset-loop не входят в v1.

Первая продуктовая приёмка остаётся неизменной: две реальные перекрывающиеся coding-подзадачи в Kaggle, события до окончания, внешние Git checkpoints, принудительная потеря одной части и продолжение из checkpoint без потери второй, интеграция точных commits и реальные проверки итогового SHA. Unit/contract tests нужны, но не заменяют этот live-путь.

Этот репозиторий — код инструмента и необходимая документация, не датасет. `data/sample_data.csv` — оставшийся исходный шаблон, не часть DAEP runtime.

## License

MIT License
