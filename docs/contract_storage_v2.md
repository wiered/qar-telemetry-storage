# Контракт Storage Core v2

Документ фиксирует публичный MVP-контракт между **ingest**, **storage**,
**analysis** и **export**. Реализация опирается на:

- код storage: `src/core/storage/`;
- ingest: `src/core/ingest.py`;
- CLI: `src/main.py`;
- тесты: `tests/storage/`, `tests/test_mvp_requirements.py`.

При расхождении между документом и реализацией источником истины считается код,
а документ должен быть обновлён при намеренном изменении поведения.

## 1. Вход ingest -> storage

`IngestService` обращается к хранилищу только через батчевый метод:

```text
append_rows(rows: list[Row]) -> None

Row = (timestamp_ns: int, columns: list[tuple[parameter_id: int, value: float]])
Point = (timestamp_ns: int, parameter_id: int, value: float)
```

Семантика:

- `timestamp_ns` - время строки в наносекундах, общее для всех значений строки;
- `columns` - ноль или больше пар `(parameter_id, value)`;
- каждая пара из `columns` превращается в отдельную точку `Point`;
- значения телеметрии в MVP представлены как `float`.

`IngestService` не зависит от внутренних API memtable, SSTable, manifest или
compaction. Его контракт с хранилищем ограничен `append_rows(...)`.

## 2. Write path

Путь записи:

1. Ingest собирает строки `Row` и передаёт их в `StorageCore.append_rows(...)`.
2. Storage разворачивает строки в точки `Point`.
3. Точки добавляются в memtable.
4. При достижении порогов строк, точек или приблизительного размера memtable
   сбрасывается в SSTable.
5. Manifest фиксирует активные SSTable.
6. Compaction объединяет SSTable и сохраняет семантику newest-wins для
   дубликатов `(timestamp_ns, parameter_id)`.

Пороги flush задаются конфигурацией `StorageSettings` /
`StorageRuntimeConfig`. Корневая директория данных задаётся через
`settings.data_dir`.

## 3. Read API

Публичная сигнатура:

```text
StorageCore.query_range(
    start_ts_ns: int,
    end_ts_ns: int,
    parameter_ids: set[int] | None = None,
) -> list[Point]
```

Семантика:

- временной фильтр - полуинтервал `[start_ts_ns, end_ts_ns)`;
- `parameter_ids=None` означает отсутствие фильтра по параметрам;
- результат отсортирован по `timestamp_ns`, затем `parameter_id`;
- read path читает snapshot текущей memtable и активные SSTable из manifest;
- дубликаты `(timestamp_ns, parameter_id)` разрешаются по newest-wins:
  memtable имеет приоритет над SSTable, более новый `table_id` имеет приоритет
  над более старым SSTable.

## 4. Analytics API

Поддерживаемые агрегаты:

```text
AggregateFunction = min | max | avg | count
```

Публичная модель результата:

```text
AggregateResult(
    start_ts_ns: int,
    end_ts_ns: int,
    parameter_id: int,
    count: int,
    min: float | None,
    max: float | None,
    avg: float | None,
)
```

Публичные функции и методы:

```text
StorageCore.aggregate_range(
    start_ts_ns: int,
    end_ts_ns: int,
    parameter_ids: set[int] | None = None,
) -> list[AggregateResult]

query_aggregates(storage, start_ts_ns, end_ts_ns, parameter_ids=None)
aggregate_points(points, start_ts_ns=..., end_ts_ns=..., parameter_ids=None)
```

`StorageCore.aggregate_range(...)` использует ту же семантику интервала и
фильтрации, что `query_range(...)`.

Поведение пустых результатов:

- если `parameter_ids` задан и для параметра нет точек, возвращается строка с
  `count = 0`, `min = None`, `max = None`, `avg = None`;
- если `parameter_ids=None` и точек нет, возвращается пустой список, потому что
  набор параметров заранее неизвестен.

## 5. Export API

Публичные функции:

```text
points_to_rows(points) -> list[dict]
aggregates_to_rows(results) -> list[dict]
write_points_csv(points, file_or_path) -> None
write_aggregates_csv(results, file_or_path) -> None
```

Порядок колонок для точек:

```text
POINT_CSV_COLUMNS = timestamp_ns, parameter_id, value
```

Порядок колонок для агрегатов:

```text
AGGREGATE_CSV_COLUMNS = start_ts_ns, end_ts_ns, parameter_id, count, min, max, avg
```

CSV export использует стандартный модуль `csv`. Аргумент `file_or_path` может
быть строковым путём, `PathLike` или уже открытым text stream.

## 6. CLI MVP

Доступные команды:

- `ingest` - запуск FDAU -> ingest -> `StorageCore`;
- `query` - чтение точек по временному диапазону;
- `aggregate` - расчёт агрегатов по временному диапазону;
- `benchmark` - benchmark harness для сравнения storage layout;
- `benchmark-scale` - scale benchmark для FDAU-sized parameter pools.

Ключевые аргументы `query` и `aggregate`:

- `--start-ts-ns` - начало полуинтервала, включительно;
- `--end-ts-ns` - конец полуинтервала, исключительно;
- `--parameter-ids` - необязательный comma-separated список параметров;
- `--output-csv` - необязательный путь для CSV export.

Примеры:

```powershell
python src/main.py ingest --duration 10 --print-stats
python src/main.py query --start-ts-ns 0 --end-ts-ns 1000000
python src/main.py query --start-ts-ns 0 --end-ts-ns 1000000 --parameter-ids 1,2,3 --output-csv out/points.csv
python src/main.py aggregate --start-ts-ns 0 --end-ts-ns 1000000
python src/main.py aggregate --start-ts-ns 0 --end-ts-ns 1000000 --parameter-ids 1,2,3 --output-csv out/aggregates.csv
python src/main.py benchmark
python src/main.py benchmark-scale
```

## 7. Durability и ограничения

Durability MVP:

- durable считаются данные, записанные в SSTable и отражённые в manifest после
  успешного сохранения manifest;
- `StorageCore.close()` штатно выполняет flush/drain текущей memtable;
- данные, оставшиеся только в memtable, могут быть потеряны при crash процесса
  до flush;
- WAL и полная crash-safety для несброшенной memtable не реализованы;
- отдельной гарантии fsync-level durability всех байтов на физическом носителе
  нет.

Явно вне MVP/v2:

- SQL-движок;
- распределённое хранение и репликация;
- полные ACID-транзакции;
- FDM/FOQA-правила предметной области;
- продвинутые вторичные индексы сверх time/parameter pruning;
- перенос storage core на Rust.

## 8. Согласование с тестами

| Поведение | Подтверждение |
|---|---|
| FDAU -> ingest -> storage smoke path | `tests/test_mvp_requirements.py` |
| Query по одному и нескольким параметрам | `tests/test_mvp_requirements.py`, `tests/storage/test_storage_core.py` |
| Aggregate API | `tests/storage/test_analysis_api.py` |
| Aggregate models | `tests/storage/test_analysis_models.py` |
| CSV export | `tests/storage/test_export.py` |
| Recovery после штатного close | `tests/storage/test_recovery.py` |
| Benchmark harness | `tests/storage/test_benchmark.py` |
