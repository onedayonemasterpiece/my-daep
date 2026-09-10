# Реализовать my-daep v1 до проверяемого распределённого coding-цикла

Постановка для нового окна ChatGPT. Дата: 2026-09-10.

## Задача и источник требований

Репозиторий: `onedayonemasterpiece/my-daep`. Последняя проверенная default branch — `master`, не `main`.
Канонический проект: [distributed-mode.md](distributed-mode.md).
На начало подготовки постановки HEAD был `65765eaf95d00cd917b75ab63a1d979e02c15ccd` — документация, без DAEP runtime. Редакция итогового проекта опубликована commit `b337162b0de081abf9fc9e802aafc1504a76a903`. Это ориентиры чтения, НЕ указание откатить ветку на них. В новом окне fresh-read repository/default branch/HEAD, другие ветки и открытые PR; продолжать фактическую реализацию, если она уже появилась.

Нужен работающий путь:

```text
Пользователь в текущем проекте OpenCode на DevCoveer явно выбирает distributed
→ supervisor сохраняет job и план
→ 1–4 Kaggle notebooks с отдельными OpenCode выполняют независимые coding-части
→ code checkpoints и результаты сохраняются в GitHub
→ события возвращают основную модель к координации
→ OpenCode объединяет точные commits и выполняет сквозные проверки
→ один итоговый PR либо фактический пакет для исправлений в ChatGPT.
```

Главный пилот: две действительно параллельные coding-части, потеря одного worker, автоматическое продолжение этой части из Git checkpoint, сохранность второго результата, итоговая интеграция и реальные тесты. Это нельзя заменить одним bootstrap, симуляцией или формулировкой «готово к запуску».

## 1. Как работать в этом окне

Source DAEP писать и исправлять лично в ChatGPT через GitHub. Не делегировать его реализацию Codex CLI/backend/model и не использовать их как fallback. OpenCode на DevCoveer допустим для чтения окружения, диагностики, запуска тестов, разрешённой установки и live acceptance. OpenCode в Kaggle при проверке ДОЛЖЕН реально писать код пилотной задачи — это проверяемая функция продукта, а не делегирование построения DAEP.

Сначала получить доступные действия GitHub и, когда нужен сервер, обнаружить актуальные действия подключённого DevCoveer-инструмента. Название коннектора «Codex DevCoveer» не означает разрешение выполнять задачу Codex: использовать именно явно выбранную OpenCode/операторскую операцию. Не объявлять соединение недоступным по памяти. Не полагаться на неоткрытые файлы или предыдущий чат.

Работать в одной implementation-ветке/PR от фактической базы, либо продолжить существующий тематический PR. Не force-push, не перезаписывать конкурентные изменения, не merge ради запуска CI. Не менять default branch. Перед финальной записью сверять изменяемые файлы/HEAD и разрешать конфликты по факту.

Не остановиться на очередной архитектуре, README, моделях данных, fake provider или наборе пустых адаптеров. Реализовать исполняемую вертикаль, проверить и сохранять source по ходу. Если внешний доступ действительно отсутствует, закончить все независимые source/test-части и вернуть конкретный оставшийся live blocker и команду его проверки. Непроверенную среду не выдавать за успешный deploy.

## 2. Непересматриваемые границы

Только Kaggle в v1; ручной/auto лимит 1–4 и общий максимум четыре. Один GitHub-repository на job, много проектов через глобальный вход. Содержание работы координирует основная модель; lifecycle и recovery обеспечивает обычный код независимо от доступности модели/окна. Распределённый режим не заменяется молча локальной разработкой.

Нет классификации исходников, DLP, запретов передачи кода моделям, согласований по классу кода. Не переносить запреты shell/Git исследовательских агентов из dataset-loop в coding-worker. Нужны обычная сохранность credentials и отсутствие перезаписи чужой работы, а не новая policy-платформа.

Не строить новый model gateway, workflow DSL, UI, Kubernetes/Redis, отдельный token service/publisher, self-hosted Actions runner, обязательный MCP/OAuth или альтернативный backend. Не мигрировать dataset-loop. Не создавать Markdown-handoff/receipt на каждый внутренний шаг; состояние и результаты проверок собираются кодом, не бюрократией.

## 3. Сначала повторно использовать подтверждённые части

Прочитать текущий dataset-loop `fix/reproducible-native-recovery-20260904` read-only. Последний источник проектирования — `0e1533057d5d8b63318c1a6841ce45063b074d33`; свежий HEAD может отличаться.

Полезные файлы:
- `src/dataset_loop_mcp/orchestration/github_access.py`: App JWT/PEM/token broker, repository scoping, expiry.
- `src/dataset_loop_mcp/api/worker.py`: worker credential, heartbeat и token-refresh endpoint.
- `src/dataset_loop_mcp/providers/kaggle_live.py` и связанные worker/bootstrap/test-файлы: launch/status/output, доставка bootstrap credentials.

Прочитать соответствующие тесты и перенести небольшой адаптированный код с нужными регрессиями. Не импортировать целиком dataset-loop service, его candidate pack, data contracts или живой token endpoint как обязательную runtime dependency. Не изменять его текущий rollout, активные notebooks, credentials или GitHub App settings без отдельной необходимости владельца. В исходном Kaggle adapter `busy_slots` не означает полную свободную ёмкость аккаунта, а `cancel_kernel` использовал delete — это не готовые реализации auto-capacity и недеструктивного stop.

## 4. Что реализовать

### A. Исполнение и durable state

Один Python-пакет/CLI и supervisor-сервис с SQLite. Job, plan version, dependency tasks, попытки, external effect intents, события и результаты сохраняются транзакционно. Идемпотентный submit, generation/lease, атомарное резервирование общего пула, bounded deadlines/retries и восстановление после рестарта. Структуру таблиц выбрать минимальную по коду; отдельный event-sourcing framework не нужен.

Сначала фиксировать намерение launch, затем внешний вызов. Timeout внешнего действия требует сверки, не слепой копии. Перезапуск одной попытки не стирает ready/completed части и не создаёт повторный job. Старый результат не меняет актуальный integration input. Видимые внешние notebooks, в том числе dataset-loop, не отменять ради DAEP.

### B. GitHub App, credentials и Git transport

Поддержать App/installation/PEM file через конфигурацию. Для пилота допускается существующая App dataset-loop с отдельным PEM-ключом; отдельная App переключается теми же настройками, без второго auth backend. Не считать новый PEM отдельной App/квотой. Фактическую установку на целевые repositories и permissions проверить; не брать ранее известные IDs/пути как доказательство готовности.

PEM остаётся на DevCoveer; notebook получает короткий repository-scoped installation token и обновляет его через broker до Git-операций. Использовать `expires_at`, credential helper, finite 401-refresh, opaque tokens без предположения о 40 символах. Авторизация token endpoint привязана к текущей попытке и её сохранённому repository. Не печатать и не коммитить credentials. У GitHub нет обещанной token-изоляции одной веткой: не подменять протокол именования фиктивной гарантией.

Contents read/write — Git, Pull requests read/write — итоговый PR; Actions/Checks/Commit statuses read — чтение проверок по потребности. Workflows write/Actions write только при реально требуемом редактировании workflows или dispatch, как возможности GitHub, не классификация кода. Webhook и `/opencode` через GitHub comments для v1 не нужны.

Точный clone/fetch base SHA, отдельные `daep/work/<job>/<task>/a<attempt>` branches, wrapper checkpoints с push/readback, восстановление из Git. Никакой общей живой ветки нескольких writers. Integration worktree и `daep/result/<job>`, один PR с защитой от дубля после lost ACK. Не добавлять traces/DB/cache в кодовые commits. Обязательные этапные saves не зависят от ответа модели.

### C. Kaggle worker и каналы наблюдения

Реальный provider adapter и notebook wrapper с закреплённым OpenCode, правильным workdir, явной моделью, bootstrap credentials, периодическими checkpoints и локальным lease watchdog. Worker инициирует HTTPS к supervisor; отдельные публичные порты notebook не нужны. Реальные OpenCode events/session status/command results плюс отдельное наблюдение Kaggle. ACK после записи; event/command ID, дубликаты и ограниченный retransmission buffer.

Поддержать stop через wrapper, подтверждение завершения и реальное освобождение ресурса; stop не подменять delete. Provider quota/очередь отличать от локального свободного слота. Последний run notebook не принимать за текущую попытку без identity. Автоматический выбор не создаёт бесполезные workers и не нарушает общий предел.

### D. Глобальный OpenCode-вход и возобновляемая координация

Глобальный tool/команда текущего worktree: submit/plan, status/follow, update/resume, cancel, export. Выдать job ID/backend/репозиторий после durable submit. Структурированный план порождает ready части и зависимости, без жёсткого автоматического N-way разбиения любой задачи.

Реализовать ОБРАТНЫЙ путь в coordinator: существенные events вызывают сериализованное продолжение session через поддержанный OpenCode API; восстановление/reattach после закрытия TUI и потери coordinator session. Хранить план/решения/прочитанный cursor, не повторять один и тот же integration effect. Если модель недоступна — явное ожидание при сохранённом lifecycle. Простая выдача job ID и status без дальнейшей интеграции не завершает этот блок.

### E. Модели, ошибки и качество

Явный model/provider ID и небольшой настроенный fallback order, разные модели coordinator/workers, конечные бюджеты/attempt limits. При `free usage exceeded` wrapper сохраняет Git checkpoint и сообщает запрос continuation/replacement. Общая quota не сбрасывается новым notebook: cooldown/другая настроенная модель/явное waiting; без бесконечного churn. Поддержать истечение GitHub token и недоступность coordinator отдельно от ошибки worker-модели.

Наблюдаемость: фактическая activity, remote checkpoint и проверки различаются; phase/reason/next action/deadline вместо вечного UNKNOWN. Новая нераспознанная ошибка попадает в общий безопасный для состояния failure path, не рушит scheduler. Тесты и модельный самоотчёт не равны успеху исходной задачи.

Интегрировать точные выбранные commits и запускать существующие project checks на объединённом SHA; различать missing infrastructure, baseline failures, ноль тестов/skip и новую регрессию. Проверить CI-фильтры, чтобы checkpoints не вызывали deployment/build storm. Не отключать финальный CI ради экономии промежуточных запусков.

### F. Backup и handoff

Автоматический restart сервиса, согласованный SQLite/config backup вне сервера и проверенный restore с Git/Kaggle reconciliation. Один активный controller; не обещать active-active HA. Конкретный backend backup и окно потери метаданных обозначить в установке; не считать backup-файл проверкой восстановления.

Один компактный результат/пакет для нового ChatGPT: исходная задача, план/решения, repo/base/result SHA, commits/diff, фактические проверки/ошибки, конкретные следующие действия. Его можно прочитать через GitHub без прежнего чата. Полные logs/session archives вне продуктового Git; нет отдельного отчёта на каждый transition. `handoff_ready` не маркировать как успешную реализацию. После внешних исправлений поддержать continuation с новой версией входа.

## 5. Проверки и порядок завершения

Реализовывать вертикально, сохраняя код. Сначала unit/contract/fault-injection и локальный OpenCode smoke; затем реальная App/Git цепь, один Kaggle canary и главный двух-worker сценарий. Fake provider и model fixtures нужны для воспроизводимости, но не заменяют реальную модель/Kaggle.

Минимальная матрица: повтор submit; lost launch ACK; lost push ACK; token refresh/expired token; duplicate events/commands; stale worker; потеря одного notebook; model quota; недоступность callback; restart controller/coordinator; cancellation без фиктивного free slot; max 1–4/auto/внешняя занятость; integration conflict; CI не запустился; restore.

Live должен подтвердить: реальные изменения исходников, пересечение времени двух notebooks, события до их завершения, внешний Git checkpoint, остановку/падение одной части и новое её исполнение от checkpoint, сохранность другой части, итоговый SHA и тесты. Выбрать небольшую полезную coding-задачу в рабочей ветке подходящего проекта; не выдавать hello-world/bootstrap/просто созданные файлы за продуктовый пилот. Активный DAEP runtime не должен переписываться собственным live-тестом без отдельного управляемого обновления.

Повторить существенную часть пути на втором проекте без изменения ядра. Поддержку лимита четырёх проверить в contract tests и реально доступных ресурсах: две live-сессии не объявлять live-four. Реальный mint/refresh и моделированное истечение tokens отмечаются отдельно; долгую работу через истечение токена не заявлять без доказательства.

Когда серверный доступ есть, выполнить установку/проверки через разрешённый OpenCode/операторский путь, не запускать Codex. При отсутствии возможностей вернуть точный one-time setup/операторский запрос и готовый проверенный source, не обещая будущую фоновую работу. Подготовка нового GitHub App key/installation и DNS/endpoint может требовать владельца; это конкретная настройка, не повод остановить остальные работы.

## 6. Что вернуть владельцу

Фактические branch/PR/HEAD, изменённый source, команды и результаты проверок, установленные версии/endpoint при реальном deploy, job/notebook/commit evidence live-пилота, что работает из обычного OpenCode сейчас, что не проверено и единственный следующий операционный шаг при blocker. Никаких заявления «промышленно готово» по числу assertions.

Обновлять README и канонический проект по факту реализации; не создавать очередную коллекцию планов и handoff-дубликатов. Реализация завершена только по приёмке в `distributed-mode.md`, а внешне заблокированная часть прямо обозначена как незавершённая, без смешения source-ready и live-ready.
