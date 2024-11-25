# Telegram Orders Bot

Бот для мониторинга и управления заявками через Telegram.

## Функционал

- Добавление и удаление Telegram аккаунтов
- Мониторинг входящих сообщений
- Автоматический парсинг заявок
- Сохранение заявок в JSON файл
- Генерация ежедневных и еженедельных отчетов

## Установка

1. Клонируйте репозиторий
2. Установите зависимости:

```bash
pip install -r requirements.txt
```

3. Создайте файл .env на основе .env.example и заполните его:

- BOT_TOKEN - токен вашего бота от @BotFather
- API_ID - ваш API ID от https://my.telegram.org
- API_HASH - ваш API Hash от https://my.telegram.org

## Использование

1. Запустите бота:

```bash
python bot.py
```

2. Команды бота:

- /start - начало работы с ботом
- /add_account - добавить новый аккаунт для мониторинга
- /daily_report - получить отчет за последние 24 часа
- /weekly_report - получить отчет за последнюю неделю

3. Добавление нового пользователя:
   Для добавляющего необходимо знать телефон, api_id и id_hash аккаунта
   Получаем тут: https://my.telegram.org/apps
   Подробнее в статье: https://habr.com/ru/companies/amvera/articles/838204/
