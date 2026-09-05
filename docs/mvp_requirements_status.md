# MVP requirements status

Документ фиксирует соответствие функциональных требований MVP текущей реализации и
плану Storage Core v2. Статусы:

- `выполнено` - требование реализовано и имеет ссылку на код или тест;
- `частично` - базовая возможность есть, но часть поведения вынесена в план v2;
- `не выполнено` - публичной реализации пока нет, есть планируемая задача.

## Матрица требований

| Область | Требование MVP | Статус | Подтверждение |
|---|---|---:|---|
| Приём данных | FDAU генерирует кадры и передаёт их в ingest callback. | выполнено | `src/core/fdau.py`, `src/main.py`, `tests/test_fdau.py` |
| Приём данных | FLT конфигурация парсится и используется для структуры параметров FDAU. | выполнено | `src/core/flt.py`, `src/main.py`, `tests/test_flt.py` |
| Приём данных | `IngestService` принимает кадры через `on_frame(...)`, управляет очередью и overflow policy. | выполнено | `src/core/ingest.py`, `tests/test_ingest.py` |
| Приём данных | Кадр FDAU преобразуется в строку `Row = (timestamp_ns, [(parameter_id, value), ...])`. | выполнено | `IngestWorker._frame_to_row(...)` в `src/core/ingest.py`, `docs/contract_storage_v2.md` |
| Буферизация и запись | Ingest пишет в storage только через `append_rows(rows)`. | выполнено | `Storage` protocol и `IngestWorker._flush()` в `src/core/ingest.py`, `docs/contract_storage_v2.md` |
| Буферизация и запись | Storage разворачивает строки ingest в точки `(timestamp_ns, parameter_id, value)`. | выполнено | `Memtable.append_rows(...)` в `src/core/storage/memtable.py`, `tests/storage/test_memtable.py` |
| Буферизация и запись | Memtable сбрасывается по порогам строк, точек или размера. | выполнено | `StorageRuntimeConfig.should_flush(...)` в `src/core/storage/config.py`, `Memtable.should_flush(...)`, `docs/contract_storage_v2.md` |
| Буферизация и запись | Ingest batch сбрасывается по числу строк и времени. | выполнено | `IngestWorker._maybe_flush_by_size(...)`, `IngestWorker._maybe_flush_by_time(...)`, `tests/test_ingest.py` |
| Буферизация и запись | Ingest batch должен учитывать `settings.ingest.batch_max_points`. | выполнено | `IngestWorker._maybe_flush_by_size(...)` в `src/core/ingest.py`, `tests/test_ingest.py::test_ingest_service_flushes_batch_by_points` |
| Хранение | Данные из memtable записываются в SSTable. | выполнено | `StorageCore.flush()`, `StorageCore._perform_flush(...)`, `src/core/storage/sstable.py`, `tests/storage/test_storage_core.py` |
| Хранение | Manifest отражает активные SSTable и используется при recovery. | выполнено | `src/core/storage/manifest.py`, `StorageCore.recover()`, `tests/storage/test_manifest.py`, `tests/storage/test_recovery.py` |
| Хранение | Поддержан recovery после штатного закрытия и повреждений manifest/SSTable в рамках v1. | выполнено | `StorageCore.close()`, `StorageCore.recover()`, `tests/storage/test_recovery.py` |
| Хранение | SSTable поддерживает time-series layout и block-level metadata для pruning. | выполнено | `src/core/storage/sstable.py`, `tests/storage/test_sstable.py`, `tests/storage/test_benchmark.py` |
| Хранение | Compaction сливает SSTable и сохраняет newest-wins семантику. | выполнено | `src/core/storage/compaction.py`, `StorageCore.compact()`, `tests/storage/test_compaction.py` |
| Чтение | Публичный read API возвращает точки за полуинтервал `[start_ts_ns, end_ts_ns)`. | выполнено | `StorageCore.query_range(...)`, `docs/contract_storage_v2.md`, `tests/storage/test_storage_core.py` |
| Чтение | Фильтрация по одному или нескольким `parameter_id`; `None` означает без фильтра по параметрам. | выполнено | `StorageCore.query_range(...)`, `tests/storage/test_storage_core.py` |
| Чтение | Результаты детерминированно отсортированы по `timestamp_ns`, затем `parameter_id`. | выполнено | `merge_runs(...)` в `src/core/storage/compaction.py`, `tests/storage/test_storage_core.py` |
| Чтение | Дубликаты `(timestamp_ns, parameter_id)` разрешаются по правилу newest-wins. | выполнено | `merge_runs(...)`, `StorageCore.query_range(...)`, `tests/storage/test_storage_core.py` |
| Чтение | Read path использует pruning файлов/блоков и собирает счётчики. | выполнено | `ManifestTableEntry.overlaps_query(...)`, `SSTableReader.iter_range(...)`, `StorageCore.stats_snapshot()`, `tests/storage/test_storage_core.py` |
| Анализ | Публичный API для `min`, `max`, `avg`, `count` по интервалу. | выполнено | `StorageCore.aggregate_range(...)`, `query_aggregates(...)`, `aggregate_points(...)`, `tests/storage/test_analysis_api.py` |
| Анализ | Единый сериализуемый формат результата агрегатов. | выполнено | `AggregateResult`, `AggregateFunction` и `aggregate_results_to_rows(...)` в `src/core/storage/analysis.py`, `tests/storage/test_analysis_models.py`. |
| Анализ | Агрегация использует ту же семантику интервала и filtering, что `query_range(...)`. | выполнено | `query_aggregates(...)` использует `StorageCore.query_range(...)`, `tests/storage/test_analysis_api.py` |
| API и экспорт | Публичный Python API записи и чтения доступен через `core.storage`. | выполнено | `src/core/storage/__init__.py`, `StorageCore.append_rows(...)`, `StorageCore.query_range(...)` |
| API и экспорт | CLI ingest использует реальный `StorageCore`. | выполнено | `fdau_ingest(...)` и команда `ingest` в `src/main.py` |
| API и экспорт | Benchmark harness доступен из CLI. | выполнено | Команда `benchmark` в `src/main.py`, `src/core/storage/benchmark.py`, `tests/storage/test_benchmark.py` |
| API и экспорт | Экспорт точек и агрегатов в табличные строки/CSV. | выполнено | `src/core/storage/export.py`, `tests/storage/test_export.py` |
| API и экспорт | CLI или demo для query/aggregate/export. | выполнено | Команды `query` и `aggregate` в `src/main.py`; обе поддерживают `--output-csv`. |
| Конфигурация | Единый объект `settings` содержит параметры FDAU, ingest и storage. | выполнено | `src/settings/__init__.py`, `StorageRuntimeConfig.from_settings(...)`, `tests/test_settings.py` |
| Конфигурация | Параметры ingest, storage и compaction вынесены в конфигурацию. | выполнено | `IngestSettings`, `StorageSettings`, `StorageRuntimeConfig` |
| Конфигурация | Отдельные профили dev/stress/prod. | не выполнено | Есть единый settings/env-механизм; профильная конфигурация не реализована как отдельная сущность. |
| Документация | Контракт ingest/storage, read API, analytics API, export API и ограничения durability описаны. | выполнено | `docs/contract_storage_v2.md`; исторический v1-контракт: `docs/contract_storage_v1.md` |
| Документация | Архитектура, CLI-сценарии и Python API MVP описаны. | выполнено | `README.md`, `docs/contract_storage_v2.md` |
| Документация | Benchmark baseline зафиксирован. | выполнено | `docs/storage_benchmark_baseline.md`, `docs/storage_scale_benchmark_baseline.md`, `docs/reports/*.json` |
| Валидация | Функциональные acceptance-тесты MVP покрывают основной пользовательский путь. | выполнено | `tests/test_mvp_requirements.py` |

## Durability

Durability в MVP соответствует актуальному `docs/contract_storage_v2.md`;
`docs/contract_storage_v1.md` оставлен как исторический контракт ingest/storage/read:

- штатное завершение через `StorageCore.close()`: метод **идемпотентен** — после
  установки закрытого состояния повторные вызовы сразу возвращаются без повторного
  drain; при первом закрытии непустая memtable ставится в очередь flush и очередь
  обслуживания сливается до конца (`_drain_maintenance_locked()` — flush и при
  необходимости compaction), тем же путём, что обычный flush/drain;
- при **успешном** завершении `close()` данные из memtable доходят до записи SSTable и
  обновления manifest так же, как при обычном flush; **ошибки записи или сохранения
  manifest пробрасываются вызывающему**, хранилище при этом уже закрыто — повторный
  `close()` не повторяет неудавшуюся работу;
- durable считаются данные, записанные в SSTable и отражённые в manifest после
  сохранения manifest (без отдельной гарантии «все байты на носителе» — буферы ОС);
- данные, которые находятся только в memtable и не были сброшены в SSTable, при crash процесса
  могут быть потеряны;
- WAL и crash-safe durability для несброшенной memtable в MVP/v2 не входят.

Подтверждение: `StorageCore.close()`, `StorageCore._perform_flush(...)`,
`StorageCore.recover()`, `tests/storage/test_storage_core.py::test_storage_core_recovers_after_close`,
`tests/storage/test_recovery.py`.

## Аналитическая модель результатов

Этап 2 Storage Core v2 фиксирует сериализуемую модель результата агрегатов:

- `AggregateFunction` задаёт публичные имена агрегатов `min`, `max`, `avg`, `count`;
- `AggregateResult` описывает одну строку результата для одного `parameter_id` на интервале
  `[start_ts_ns, end_ts_ns)`;
- `aggregate_results_to_rows(...)` возвращает обычные `dict`-строки с устойчивым порядком ключей:
  `start_ts_ns`, `end_ts_ns`, `parameter_id`, `count`, `min`, `max`, `avg`.

Для пустого диапазона по явно запрошенному параметру аналитический API возвращает
строку с `count = 0` и `None` для `min`, `max`, `avg`. Если `parameter_ids=None` и точек нет,
результат агрегации будет пустым списком, потому что набор параметров заранее неизвестен.

## Ограничения v2

Следующие ограничения зафиксированы планом v2 и не противоречат
`docs/contract_storage_v2.md`:

- нет SQL-движка;
- нет WAL и полной crash-safety для данных, оставшихся только в memtable;
- нет распределённого хранения и репликации;
- нет продвинутых индексов сверх time/parameter pruning, используемого текущим read path;
- нет FDM/FOQA-правил предметной области;
- нет графического интерфейса;
- нет переноса storage core на Rust.

## Синхронизация с roadmap MVP

Актуальный roadmap: `docs/obsidian/MVP/Roadmap разработки проекта (MVP → развитие).md`.

По состоянию на текущую реализацию закрыты этапы:

- интеграция источника данных;
- ingest-слой и подключение к реальному `StorageCore`;
- базовое LSM-хранилище: memtable, SSTable v1/v2, manifest, recovery;
- минимальный compaction;
- чтение по времени и параметрам;
- агрегаты `min`, `max`, `avg`, `count`;
- CLI query/aggregate/export/benchmark;
- конфигурация ingest/storage/compaction;
- функциональные MVP-тесты и benchmark baseline;
- документация README/contract/status.

Оставшиеся пункты roadmap, не блокирующие MVP:

- отдельный time-based flush для memtable; сейчас time-based flush есть на уровне ingest batch, а memtable flush срабатывает по строкам, точкам и приблизительному объёму;
- отдельные профили конфигурации dev/stress/prod;
- развитие после MVP: полноценное сжатие данных, расширенная индексация параметров, SQL-подобные запросы, перенос ядра на Rust, FDM/FOQA-логика.
