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

## Tor, VPN-ноды для url-check, несколько коллекторов

**Tor вместо proxies.txt.** В `.env`: `TOR_INSTANCES=20` и `COMPOSE_PROFILES=tor`, затем
`docker compose up -d --build`. Контейнер `tor` поднимает N клиентов (порты 9050…), парсер на каждый
запрос берёт случайный SOCKS-логин, и Tor строит для него отдельную цепочку (другой exit IP). NEWNYM не
нужен (он ограничен: 1 раз в 10 с на инстанс). Каждому инстансу нужно ~60–100 МБ ОЗУ. Первый запуск
качает консенсус несколько минут. При упоре в блокировки exit-нод можно ограничить страны:
`TOR_EXIT_NODES={de},{nl}`. Таймаут и число попыток для Tor: `tor_http_timeout_seconds` (30),
`tor_http_attempts` (5) в `config.yaml`. Автонастройка в режиме Tor не снижает поток из-за блокировок.

**VPN для url-check.** `cp vpn_nodes.txt.example vpn/nodes.txt`, по одной `vless://` ссылке в строке
(tls / reality, tcp / ws / httpupgrade / grpc / h2 / xhttp). Внутри контейнера парсера запускается Xray, каждая
нода получает локальный SOCKS-порт, при старте все ноды измеряются реальным запросом url-check, берётся самая
быстрая. Нода упала: сразу берётся следующая по скорости, упавшая перепроверяется с нарастающей паузой,
раз в 10 минут все ноды меряются заново. Если в файле нет нод или Xray не стартовал, url-check идёт
через пул прокси, как раньше. Ссылки содержат ваш id: `vpn/nodes.txt` в git не попадает.

**Несколько коллекторов.** `STORE_ENDPOINT=192.168.0.101:9000,192.168.0.102:9000` (путь `/store_items`
добавится сам). Батчи идут по кругу, недоступный коллектор пропускается.

**Горячие посты.** Профиль только показывает, какие посты есть и сколько у них ответов (`observe`), а
страницы постов открываются отдельно, по ожидаемой отдаче (`scraper/hotposts.py`): новый пост или рост
ответов на `reply_min_growth` и больше открывается сразу, пост, который даёт новые комментарии, опрашивается
чаще, пустой откладывается всё дальше и после `hot_max_misses` пустых открытий забывается. Состояние
в `data/hot_posts.json`. Настройки (все необязательные): `hot_posts` (true), `hot_posts_per_cycle` (2000),
`hot_min_interval_seconds` (90), `hot_base_interval_seconds` (300), `hot_max_misses` (5). В логе строка
`hot posts: pages=.. new=.. (x new/page)`. Выключить: `hot_posts: false`.

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
