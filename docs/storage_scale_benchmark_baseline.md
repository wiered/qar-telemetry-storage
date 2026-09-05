# Storage Scale Benchmark Baseline

## Назначение

Этот baseline фиксирует отдельный scale benchmark для Storage Core v2 на FDAU-sized наборах параметров. Он дополняет основной benchmark и отвечает на вопрос, выдерживает ли storage запись кадров, где в одном FDAU tick приходит много значений.

Scale benchmark не является performance SLA. Он использует синтетический dataset и проверяет stress-сценарий `points_per_row == parameter_pool`.

## Как повторить

Каноническая команда для Windows PowerShell:

```powershell
.\.venv\Scripts\python.exe src\main.py benchmark-scale `
  --seed 42 `
  --rows 512 `
  --pools 200,500,1000,3000 `
  --block-max-points 256 `
  --output-json reports/storage_scale_benchmark_baseline.json
```

Если `.venv` недоступен:

```powershell
python src\main.py benchmark-scale `
  --seed 42 `
  --rows 512 `
  --pools 200,500,1000,3000 `
  --block-max-points 256 `
  --output-json reports/storage_scale_benchmark_baseline.json
```

## Параметры набора данных

| Параметр | Значение |
| --- | ---: |
| seed | 42 |
| rows per pool | 512 |
| parameter pools | 200, 500, 1000, 3000 |
| points per row | равно parameter pool |
| block max points | 256 |
| SSTable format | `v2_timeseries` |

Окружение сохранённого запуска:

| Параметр | Значение |
| --- | --- |
| created_at_utc | 2026-04-29T00:07:05.546356+00:00 |
| python_version | 3.10.11 |
| platform | Windows-10-10.0.19045-SP0 |
| processor | AMD64 Family 25 Model 33 Stepping 2, AuthenticAMD |

## Сценарии

Scale benchmark запускает только `v2_timeseries`:

| Сценарий | Что измеряет |
| --- | --- |
| `ingest_only` | Чистую запись stress-кадров, rows/s, points/s, bytes/point и flush count. |
| `wide_query` | Широкий scan path по всем или половине параметров. |
| `cold_recovery_startup` | Время восстановления storage после закрытия и повторного открытия. |

## Результаты конкретного запуска

| Parameter pool | Rows | Points | ingest rows/s | ingest points/s | bytes/point | wide p95 ms | recovery ms |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 200 | 512 | 102080 | 2620.9 | 522544.2 | 12.511 | 145.725 | 1.653 |
| 500 | 512 | 255683 | 1029.1 | 513918.0 | 15.397 | 387.978 | 2.732 |
| 1000 | 512 | 511685 | 497.2 | 496930.9 | 15.202 | 837.714 | 2.601 |
| 3000 | 512 | 1535688 | 145.4 | 436191.8 | 15.068 | 2494.806 | 2.791 |

Полный отчёт сохранён в
`reports/storage_scale_benchmark_baseline.json`.

## Интерпретация для MVP

Запись выдерживает пулы 200, 500, 1000 и 3000 параметров с большим запасом для
текущего `settings.hz=8`: даже для 3000 параметров это около 24k points/s
входящей FDAU-нагрузки против 436k points/s в storage-only benchmark.

Wide query latency растёт пропорционально объёму сканирования и на 3000
параметрах достигает секундного диапазона. Для MVP это не блокирует ingest, но
означает, что широкие аналитические запросы по большим окнам не стоит считать
интерактивными без отдельной оптимизации или ограничения окна/набора параметров.

## Ограничения интерпретации

- Benchmark измеряет storage-only путь, а не полный FDAU -> ingest -> storage
  pipeline.
- Dataset синтетический и stress-oriented: все параметры активны в каждой
  строке.
- Абсолютные значения throughput и latency зависят от CPU, диска, ОС, версии
  Python и фоновой нагрузки.
