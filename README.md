# threads_parser

Скрапер Threads.net для Exorde: workers, SeenCache, TokenBucket, batch POST в
`/store_items`. Два режима (`mode` в `config.yaml` или env `MODE`):

- **http** (по умолчанию): без браузера и без аккаунтов. Публичные страницы Threads
  отдают посты и ответы в HTML (`<script data-sjs>`), их берёт обычный HTTP-запрос
  (`curl_cffi`, отпечаток Chrome) через пул ротируемых прокси из `proxies.txt`.
  Лёгкий по памяти, масштабируется числом прокси и `http_concurrency`.
- **browser**: Playwright + вход в аккаунт (`storage_state`) + VPN на аккаунт (Xray).

## Быстрый старт (http)

```bash
cp proxies.txt.example proxies.txt      # свои прокси, по одной в строке
cp targets.txt.example targets.txt      # доп. профили (необязательно), по одному в строке
cp .env.example .env
docker compose up -d --build parser
docker compose logs -f parser           # строка "http cycle: ..." раз в цикл
```

`proxies.txt`, `targets.txt`, `.env` — секреты/локальные файлы, в git не попадают.

## Исследовательские скрипты

`scripts/threads_http_probe.py`, `anon_probe.py`, `dump_page.py`, `tiktok_probe.py`,
`tiktok_discover.py` — разведка страниц (см. docstring в каждом).

## Структура

```
.
├── main.py                   # точка входа: конфиг, общие объекты, запуск воркеров
├── scraper/
│   ├── __init__.py
│   ├── constants.py          # пути, таймауты, логгер, константы (DOMAIN=threads.net)
│   ├── config.py             # ConfigStore (hot-reload config.yaml), load_accounts, load_cookies
│   ├── state.py               # TokenBucket, SeenCache (claim/confirm/release)
│   ├── pipeline.py           # build_payload (схема Exorde) + send_batch (/store_items)
│   ├── worker.py             # account_worker (цикл на аккаунт), заглушка v1
│   └── browser.py            # Playwright-сессия: proxy + cookies, заглушка fetch_new_posts
├── config.yaml                # тюнинг: target_rate, poll_interval, targets, batch_max_items...
├── accounts.yaml              # слоты аккаунтов: cookie_file + proxy (креды из .env) + enabled
├── cookies/account_N.json     # cookies аккаунтов (Cookie Editor export) — НЕ коммитить
├── requirements.txt
└── .env.example
```

## Browser mode (статус)

`fetch_new_posts()` реализован — но не через ручную сборку GraphQL-запросов
(там часть заголовков вроде `x-fb-lsd`/`x-asbd-id`/`x-bloks-version-id`
завязана на конкретную сессию/сборку клиента и их небезопасно
хардкодить/угадывать), а через **перехват настоящих ответов**: Playwright
открывает залогиненную страницу профиля и скроллит её, а мы слушаем
`page.on("response")` и разбираем JSON тех GraphQL-ответов, которые
страница и так шлёт сама (`BarcelonaFeedDirectQuery`/`...Pagination...`
для ленты, `BarcelonaPostPageTargetQuery`/`...Downward...` для реплаев).
Разбор путей в ответе — по данным из реальной авторизованной сессии
(доки/пример структуры — см. `docs/threads_graphql_examples`, если
захотите свериться).

Что уже работает:
- лента профиля (`targets: type: "profile"`) — top-level посты;
- прямые (direct) реплаи каждого поста, у которого `direct_reply_count > 0`
  (по умолчанию включено, `config.yaml:fetch_replies`).

Что сознательно НЕ реализовано в v1 (осталось на потом, чтобы не
плодить недоделанное на живых данных):
- target type `"post"`/`"thread"` — точечный тред по ссылке (сейчас только
  `"profile"`);
- цепочка родителей вверх по треду (`BarcelonaPostPageUpwardQuery`);
- глубокая довыгрузка реплаев "show more replies"
  (`...RepliesRefetchQuery`) — берутся только те, что подгружаются сами
  при открытии поста, без доп. пагинации.

Конфиг, дедуп, рейт-лимит, батчинг, сборка payload под схему Exorde —
рабочее и не зависит от изменений выше.

## Схема payload (Exorde)

```
{
  "content": str,              # текст поста/реплая
  "external_id": str,          # реальный id поста из Threads
  "created_at": str,           # ISO8601, реальное время публикации
  "url": str,                  # реальная ссылка на пост
  "title": str,                # обычно "" — у Threads нет заголовков постов
  "author": str,                # display name
  "username": str,              # handle без @
  "external_parent_id": str,   # "" для top-level поста, id родителя для reply
  "domain": "threads.net",
  "summary": str                 # JSON-строка (не объект!) с доп. метаданными
}
```

Правило parent_id: top-level post → `external_parent_id = ""`;
reply → `external_parent_id` = реальный id родительского поста (не автора,
не темы — именно поста, на который отвечают).

## Запуск (без Docker, для разработки)

```bash
pip install -r requirements.txt
playwright install chromium
cp .env.example .env   # заполнить STORE_ENDPOINT при необходимости
python3 main.py
```

Локальный коллектор Exorde (`store_items`) должен слушать на
`STORE_ENDPOINT` (по умолчанию `http://127.0.0.1:9000/store_items`).
Парсер никогда не шлёт напрямую на `upload.exorde.network`.

## Cookies

1. Залогинься в Threads в браузере под нужным аккаунтом.
2. Экспортируй cookies расширением Cookie Editor → Export → JSON.
3. Сохрани как `cookies/account_1.json` (и т.д. для остальных аккаунтов).
4. В `accounts.yaml` выстави этому аккаунту `enabled: true` и укажи
   `proxy_user_env`/`proxy_pass_env` — реальные значения только в `.env`.

`cookies/` в `.gitignore` — файлы с сессиями в git не попадают.

## Proxy

Обязателен для каждого аккаунта (см. `accounts.yaml`). Креды прокси —
только через переменные окружения из `.env`, в конфиге — плейсхолдеры
(имена env-переменных), не значения.

## Тюнинг сбора (в `config.yaml`)

- `max_feed_scrolls` — сколько раз проскроллить ленту профиля за цикл,
  чтобы страница сама подгрузила следующие страницы (пагинация — это
  ответственность самой страницы, мы её не эмулируем руками).
- `scroll_pause_seconds` — пауза между скроллами / после открытия поста,
  чтобы JS страницы успел сходить в GraphQL и мы успели поймать ответ.
- `fetch_replies` — открывать ли каждый пост с `direct_reply_count > 0`
  отдельно, чтобы забрать его прямые реплаи (доп. переход на страницу на
  каждый такой пост — дороже по времени и трафику через прокси).

## Что нужно от вас дальше

1. `cookies/account_1.json` — уже положил присланный вами Cookie Editor
   export в репозиторий (домен `.threads.com` — это реальный хост сайта,
   `domain: "threads.net"` в payload — фиксированное поле схемы Exorde,
   они не связаны).
2. Прокси на аккаунт (host/port/протокол — реальные креды только в
   `.env`, в `accounts.yaml` сейчас плейсхолдер `proxy.example.com:8000` и
   `enabled: false` — выставьте оба, когда добавите прокси).
3. Уточнить список целей в `config.yaml:targets` (сейчас плейсхолдер
   `zuck`) — на каких профилях реально нужен сбор.
4. Прогнать v1 разово вручную (не в проде) и свериться, что реальные
   посты/реплаи долетают до `store_items` в ожидаемом виде — структура
   ответа бралась из одной сессии, Threads может её слегка менять.