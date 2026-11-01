# Сервис мониторинга и обнаружения аномалий

Локальный прототип на Python: сбор системных метрик, обработка access-логов
и поиск отклонений с помощью инкрементального PCA на основе SVD.

**Стек:** Python 3.10+, NumPy, SQLite, HTTP/JSON, HTML/CSS/JavaScript.
**Статус:** в разработке, есть рабочий локальный прототип.

## Возможности

- Сбор системных показателей Windows/Linux и приём данных через HTTP API.
- Чтение логов заданного формата: частота запросов, среднее время ответа, доля 5xx.
- Оценка отклонений, сохранение наблюдений и состояния в SQLite.
- Веб-интерфейс, демонстрационный поток и учебное веб-приложение.

## Быстрый запуск

Из корня проекта в PowerShell:

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe -m monitor_service --data-dir .\runtime-data
```

Откройте **http://127.0.0.1:8765**. Остановка — `Ctrl+C`.
Данные создаются в `runtime-data/` и не отслеживаются Git.
На Linux используются `python3` и `.venv/bin/python`.

## Структура

```text
monitor_service/
  __main__.py          # запуск сервиса
  server.py            # HTTP API
  store.py             # SQLite и сохранение состояния
  detector.py          # обнаружение отклонений
  _reference_models.py # NumPy-реализация IPCA/SVD
  log_*.py             # чтение и обработка журналов
  static/              # веб-интерфейс
  examples/            # примеры входных данных
  test_*.py            # тесты и учебное приложение test_app.py
requirements.txt
```

## Проверка

```powershell
.\.venv\Scripts\python.exe -m unittest monitor_service.test_log_parser monitor_service.test_log_aggregation
```

Остальные тесты сохранены на своих местах. Об условиях их запуска и
исследовательских модулях — [в примечаниях](docs/NOTES.md).

Сервис доступен только на этом компьютере. Сигнал модели означает отклонение,
а не установленную причину сбоя; качество на производственных авариях не заявляется.
