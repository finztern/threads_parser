# threads_parser

Парсер Threads.net для Exorde: публичные страницы Threads отдают посты и ответы в HTML
(`<script data-sjs>`), их берёт обычный HTTP-запрос (`curl_cffi`, отпечаток Chrome)
через пул прокси из `proxies.txt`. Без браузера и без аккаунтов. Данные уходят батчами
POST в локальный коллектор `/store_items` (`STORE_ENDPOINT`), никогда напрямую на
`upload.exorde.network`.

## Быстрый старт

```bash
cp proxies.txt.example proxies.txt      # свои прокси, по одной в строке
cp targets.txt.example targets.txt      # доп. профили (необязательно), по одному в строке
cp .env.example .env
docker compose up -d --build parser
docker compose logs -f parser           # строка "http cycle: ..." раз в цикл
```

`proxies.txt`, `targets.txt`, `.env`, `data/` — локальные файлы, в git не попадают.

Без Docker: `pip install -r requirements.txt && python3 main.py`.

## Как работает

- `scraper/http_client.py` — запрос профиля/поста через следующий прокси пула; бан прокси
  при ошибках (таймаут — короткий, отказ соединения — длинный).
- `scraper/threads_parse.py` — разбор встроенных в HTML данных в посты/ответы.
- `scraper/discovery.py` — автонабор профилей: авторы полученных постов добавляются в очередь;
  активные проверяются часто, молчащие реже, несуществующие отбрасываются. Список хранится в
  `data/targets_state.json`.
- `scraper/worker.py` — фильтр по возрасту, дедуп (SeenCache), TokenBucket, отправка батчей.
- `scraper/pipeline.py` — сборка payload по схеме Exorde и `send_batch`.
- Тюнинг — `config.yaml` (hot-reload): `http_concurrency`, `target_rate_per_second`,
  `http_timeout_seconds`, `discovery.*`, `batch_max_items`.

`scripts/threads_http_probe.py` — разведочный скрипт (один прокси из `PROBE_PROXY`).

Проверка TCP-порта прокси:

```bash
python3 scripts/test_proxies.py
```

Для проверки прокси-запросом укажи URL health endpoint, которым ты управляешь:

```bash
python3 scripts/test_proxies.py --url "https://your-host/health"
```

Активная проверка делает один GET без перехода по редиректам через каждый прокси и
сохраняет ответы 2xx в `proxy-results/proxies.working.txt`. Затем укажи
`/proxy-results/proxies.working.txt` в `config.yaml` как `proxies_file` и перезапусти
парсер. По умолчанию одновременно выполняется не более 8 проверок;
таймаут и параллельность меняются через `--timeout` и `--workers`. TCP-режим проверяет
только доступность порта, а не работу прокси-протокола или авторизацию.

## Схема payload (Exorde)

```
{
  "content": str,              # текст поста/реплая
  "external_id": str,          # реальный id поста из Threads
  "created_at": str,           # UTC, "YYYY-MM-DDTHH:MM:SSZ"
  "url": str,                  # реальная ссылка на пост
  "title": str,                # "" — у Threads нет заголовков
  "author": str,               # display name
  "username": str,             # handle без @
  "external_parent_id": str,   # "" для top-level, id родительского поста для reply
  "domain": "threads.net",
  "summary": str               # JSON-строка (не объект!)
}
```

## Не реализовано

- Глубокая пагинация ответов («show more replies»): берутся только ответы первой страницы поста.
