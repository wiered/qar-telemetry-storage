# Storage Benchmark Baseline

## Назначение

Этот baseline фиксирует воспроизводимый запуск benchmark для Storage Core v2.
Он нужен для MVP-валидации:

- сравнения форматов SSTable `v1_raw` и `v2_timeseries`;
- проверки ingest, query, compaction и recovery сценариев;
- подтверждения, что query path отдаёт counters для file/block pruning;
- дальнейших сравнений после оптимизаций.

Baseline не является performance SLA. Абсолютные latency и throughput зависят от
CPU, диска, ОС, версии Python и фоновой нагрузки.

## Как повторить

Каноническая команда для Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe src\main.py benchmark `
  --seed 42 `
  --rows 8192 `
  --parameter-pool 32 `
  --points-per-row 8 `
  --block-max-points 256 `
  --sweep 128,256,512,1024 `
  --output-json reports/storage_benchmark_baseline.json
```

Если `.venv` недоступен:

```powershell
python src\main.py benchmark `
  --seed 42 `
  --rows 8192 `
  --parameter-pool 32 `
  --points-per-row 8 `
  --block-max-points 256 `
  --sweep 128,256,512,1024 `
  --output-json reports/storage_benchmark_baseline.json
```

Команда создаёт машинно-читаемый отчёт:
`reports/storage_benchmark_baseline.json`.

## Параметры набора данных

Параметры запуска:

| Параметр | Значение |
| --- | ---: |
| seed | 42 |
| rows | 8192 |
| total points | 65619 |
| parameter pool | 32 |
| target points per row | 8 |
| block max points | 256 |
| sweep candidates | 128, 256, 512, 1024 |

Окружение конкретного сохранённого запуска:

| Параметр | Значение |
| --- | --- |
| created_at_utc | 2026-04-28T23:55:22.546312+00:00 |
| python_version | 3.10.11 |
| platform | Windows-10-10.0.19045-SP0 |
| processor | AMD64 Family 25 Model 33 Stepping 2, AuthenticAMD |

## Сценарии

Benchmark harness запускает одинаковые сценарии для `v1_raw` и
`v2_timeseries`.

| Сценарий | Что измеряет |
| --- | --- |
| `ingest_only` | Чистую запись, rows/s, points/s, bytes/point и flush count. |
| `narrow_query` | Узкие запросы по коротким окнам и малому набору параметров; ключевой сценарий для pruning counters. |
| `wide_query` | Широкие запросы по большим окнам и большему набору параметров; показывает scan path. |
| `mixed_read_write` | Лёгкую конкурентную запись и чтение. |
| `frequent_flush_compact` | Частые flush и compaction, rewrite объём и bytes/point. |
| `cold_recovery_startup` | Время восстановления storage после закрытия и повторного открытия. |

## Сравнение форматов

`v1_raw` используется как baseline layout. `v2_timeseries` используется как
оптимизированный time-series layout. Для MVP сравниваются:

- `rows/s`;
- `points/s`;
- `bytes/point`;
- `p50` и `p95` latency;
- `files_opened` и `files_pruned`;
- `blocks_scanned` и `blocks_pruned`.

В этом запуске `v2_timeseries` уменьшил размер записи на точку:

- `ingest_only`: 20.765 -> 11.711 bytes/point;
- `frequent_flush_compact`: 914.608 -> 514.612 bytes/point.

Query latency в этом конкретном окружении быстрее у `v1_raw`, поэтому результат
нужно трактовать как baseline для последующих оптимизаций, а не как доказательство
превосходства `v2_timeseries` по всем метрикам. Pruning counters совпадают между
форматами, что подтверждает работу metadata-based pruning в обоих layout.

## Результаты конкретного запуска

Краткая таблица по основным сценариям:

| Format | Scenario | rows/s | points/s | bytes/point | p50 ms | p95 ms |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| `v1_raw` | `ingest_only` | 37046.5 | 296747.1 | 20.765 | 0.000 | 0.000 |
| `v1_raw` | `narrow_query` | 853.6 | 6401.8 | 0.000 | 1.209 | 1.271 |
| `v1_raw` | `wide_query` | 147.4 | 425092.0 | 0.000 | 6.047 | 12.332 |
| `v1_raw` | `frequent_flush_compact` | 973.6 | 7798.8 | 914.608 | 0.000 | 0.000 |
| `v2_timeseries` | `ingest_only` | 33894.3 | 271497.4 | 11.711 | 0.000 | 0.000 |
| `v2_timeseries` | `narrow_query` | 628.6 | 4714.4 | 0.000 | 1.549 | 2.242 |
| `v2_timeseries` | `wide_query` | 103.0 | 297081.0 | 0.000 | 8.937 | 13.667 |
| `v2_timeseries` | `frequent_flush_compact` | 711.9 | 5702.3 | 514.612 | 0.000 | 0.000 |

Narrow query pruning counters:

| Format | files_pruned | files_opened | blocks_pruned | blocks_scanned | points_returned |
| --- | ---: | ---: | ---: | ---: | ---: |
| `v1_raw` | 12 | 12 | 2922 | 18 | 90 |
| `v2_timeseries` | 12 | 12 | 2922 | 18 | 90 |

Wide query scan counters:

| Format | files_pruned | files_opened | blocks_pruned | blocks_scanned | points_returned |
| --- | ---: | ---: | ---: | ---: | ---: |
| `v1_raw` | 8 | 8 | 1834 | 126 | 23070 |
| `v2_timeseries` | 8 | 8 | 1834 | 126 | 23070 |

Block-size sweep для `v2_timeseries` выбрал `block_max_points=256`.

| block_max_points | ingest rows/s | narrow p95 ms | bytes/point | compaction ms |
| ---: | ---: | ---: | ---: | ---: |
| 128 | 33765.6 | 2.610 | 567.250 | 9977.9 |
| 256 | 32325.4 | 1.598 | 514.612 | 9636.4 |
| 512 | 35904.5 | 2.200 | 487.966 | 9663.8 |
| 1024 | 34459.6 | 2.985 | 474.714 | 9396.8 |

Полный отчёт сохранён в `reports/storage_benchmark_baseline.json`.

## Ограничения интерпретации

- Benchmark использует синтетический dataset, а не реальные QAR/FDAU traces.
- Абсолютные значения throughput и latency нельзя переносить на другую машину без
  повторного запуска.
- Baseline фиксирует состояние MVP и нужен для сравнения с будущими изменениями.
- Unit-тест `tests/storage/test_benchmark.py` остаётся малым smoke-тестом harness-а
  и не запускает canonical baseline на 8192 rows.
