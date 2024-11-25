import asyncio
import json
import logging
import re
import sys
import traceback
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message
from decouple import config
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.handlers import MessageHandler

load_dotenv()

# Конфигурация бота
BOT_TOKEN = config("BOT_TOKEN")
ADMINS = config('ADMINS').split(',')

# defining the timezone
tz = pytz.timezone('Europe/Moscow')

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Логгирование
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)


# Состояния FSM
class UserStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_code = State()


# Словарь для хранения клиентов Pyrogram
pyrogram_clients = {}

# Словарь для хранения временных данных клиентов
client_temp_data = {}


# Функции для работы с аккаунтами
def load_accounts():
    try:
        with open('accounts.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_accounts(accounts):
    with open('accounts.json', 'w', encoding='utf-8') as f:
        json.dump(accounts, f, ensure_ascii=False, indent=4)


# Функция для загрузки заявок
def load_orders():
    try:
        with open('orders.json', 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


# Функция для сохранения заявок
def save_orders(orders):
    with open('orders.json', 'w', encoding='utf-8') as f:
        json.dump(orders, f, ensure_ascii=False, indent=4)


# Функция для инициализации клиентов Pyrogram
async def init_account(phone, data):
    await disable_active_account(phone)
    try:
        async with Client(
                f"session_{phone}",
                api_id=data['api_id'],
                api_hash=data['api_hash'],
                phone_number=phone
        ) as client:
            pyrogram_clients[phone] = client
            print(f"Запуск мониторинга для клиента {client.phone_number}")
            client.add_handler(MessageHandler(handle_message, filters.text & ~filters.me))
            await idle()

    except Exception as e:
        print(f"Ошибка при инициализации клиента {phone}: {str(e)}")
        traceback.print_exc()


# Функция для остановки клиентов Pyrogram
async def disconnect_account(phone, data):
    try:
        async with Client(
                f"session_{phone}",
                api_id=data['api_id'],
                api_hash=data['api_hash'],
                phone_number=phone
        ) as client:
            client.disconnect()
    except Exception as e:
        print(f"Ошибка при отключении клиента {phone}: {str(e)}")
        traceback.print_exc()


# Проверка на авторизацию
async def is_user_authorized(client):
    try:
        await client.get_me()
        return True
    except Exception as e:
        return False


# Оповещение администрации
async def wakeup_admins(message):
    for admin in ADMINS:
        await bot.send_message(chat_id=admin, text=message)


# Отключение активного аккаунта:
async def disable_active_account(phone):
    if phone in pyrogram_clients:
        try:
            await pyrogram_clients[phone].stop()
        except ConnectionError as e:
            if "Client is already terminated" in str(e):
                pass
            else:
                traceback.print_exc()
                await wakeup_admins(f"Ошибка при отключении аккаунта ({phone})")

        try:
            await pyrogram_clients[phone].disconnect()
        except ConnectionError as e:
            if "Client is already disconnected" in str(e):
                pass
            else:
                traceback.print_exc()
                await wakeup_admins(f"Ошибка при отключении аккаунта ({phone})")
        del pyrogram_clients[phone]


# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(message: Message):
    commands_text = """
Привет! Я бот для мониторинга заявок. Вот список доступных команд:

/add_account - Добавить новый аккаунт для мониторинга
/remove_account - Удалить существующий аккаунт
/daily_report - Получить отчет за последние 24 часа
/weekly_report - Получить отчет за последнюю неделю
/report day
/report week
Для начала работы добавьте аккаунт с помощью команды /add_account
"""
    await message.answer(commands_text)


# Обработчик команды добавления аккаунта
@dp.message(Command("add_account"))
async def cmd_add_account(message: Message, state: FSMContext):
    await state.set_state(UserStates.waiting_for_phone)
    await message.answer("Введите номер телефона, к которому привязан Telegram аккаунт:")


# Обработчик ввода номера телефона
@dp.message(UserStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip()

    # Сохраняем номер телефона во временные данные
    client_temp_data[phone] = {}
    await state.update_data(phone=phone)

    # Запрашиваем API ID
    await state.set_state(UserStates.waiting_for_api_id)
    await message.answer("Введите API ID (можно получить на https://my.telegram.org):")


# Обработчик ввода API ID
@dp.message(UserStates.waiting_for_api_id)
async def process_api_id(message: Message, state: FSMContext):
    try:
        api_id = int(message.text.strip())
        data = await state.get_data()
        phone = data['phone']

        # Сохраняем API ID во временные данные
        client_temp_data[phone]['api_id'] = api_id
        await state.update_data(api_id=api_id)

        # Запрашиваем API Hash
        await state.set_state(UserStates.waiting_for_api_hash)
        await message.answer("Введите API Hash (можно получить на https://my.telegram.org):")
    except ValueError:
        await message.answer("API ID должен быть числом. Попробуйте еще раз:")


# Обработчик ввода API Hash
@dp.message(UserStates.waiting_for_api_hash)
async def process_api_hash(message: Message, state: FSMContext):
    api_hash = message.text.strip()
    data = await state.get_data()
    phone = data['phone']
    api_id = data['api_id']

    # Сохраняем API Hash во временные данные
    client_temp_data[phone]['api_hash'] = api_hash

    await message.answer("Попытка входа...")

    try:
        client = Client(
            f"session_{phone}",
            api_id=api_id,
            api_hash=api_hash,
            phone_number=phone
        )
        await client.connect()

        if not await is_user_authorized(client):
            sent_code = await client.send_code(phone)
            await state.update_data(client=client, sent_code=sent_code)
            await state.set_state(UserStates.waiting_for_code)
            await message.answer("Введите код подтверждения:")
        else:
            pyrogram_clients[phone] = client
            # Сохраняем аккаунт со всеми данными
            accounts = load_accounts()
            data = {
                "api_id": api_id,
                "api_hash": api_hash,
                "added_at": datetime.now().strftime("%Y.%m.%d %H:%M:%S")
            }
            accounts[phone] = data
            save_accounts(accounts)
            # Очищаем временные данные
            if phone in client_temp_data:
                del client_temp_data[phone]
            await message.answer("Аккаунт успешно добавлен!")

            await asyncio.create_task(init_account(phone, data))
            await state.clear()

    except Exception as e:
        traceback.print_exc()
        await wakeup_admins("Произошла ошибка (Обработчик ввода API Hash)")
        if phone in client_temp_data:
            del client_temp_data[phone]
        await state.clear()


# Обработчик ввода кода подтверждения
@dp.message(UserStates.waiting_for_code)
async def process_code(message: Message, state: FSMContext):
    code = message.text.strip()
    data = await state.get_data()
    phone = data['phone']

    try:
        await data['client'].sign_in(
            phone_number=phone,
            phone_code_hash=data['sent_code'].phone_code_hash,
            phone_code=code
        )
        client = data['client']
        pyrogram_clients[phone] = client

        # Сохраняем аккаунт со всеми данными
        accounts = load_accounts()
        data = {
            "api_id": client_temp_data[phone]['api_id'],
            "api_hash": client_temp_data[phone]['api_hash'],
            "added_at": datetime.now().strftime("%Y.%m.%d %H:%M:%S")
        }
        accounts[phone] = data
        save_accounts(accounts)

        await data['client'].disconnect()

        # Очищаем временные данные
        if phone in client_temp_data:
            del client_temp_data[phone]

        await message.answer("Аккаунт успешно добавлен!")

        await asyncio.create_task(init_account(phone, data))

    except Exception as e:
        traceback.print_exc()
        await wakeup_admins("Обработчик ввода кода подтверждения")
        if phone in client_temp_data:
            del client_temp_data[phone]

    await state.clear()


# Обработчик команды для удаления аккаунта
@dp.message(Command("remove_account"))
async def cmd_remove_account(message: Message):
    accounts = load_accounts()
    if not accounts:
        await message.answer("Нет добавленных аккаунтов.")
        return

    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text=phone)] for phone in accounts],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer("Выберите аккаунт для удаления:", reply_markup=keyboard)


# Обработчик выбора аккаунта для удаления
@dp.message(lambda message: message.text in load_accounts())
async def process_remove_account(message: Message):
    phone = message.text
    accounts = load_accounts()

    if phone in accounts:
        # Отключаем клиент если он активен
        await disable_active_account(phone)

        # Удаляем из accounts.json
        del accounts[phone]
        save_accounts(accounts)

        await message.answer(
            f"Аккаунт {phone} успешно удален.",
            reply_markup=types.ReplyKeyboardRemove()
        )


    else:
        await message.answer(
            "Аккаунт не найден.",
            reply_markup=types.ReplyKeyboardRemove()
        )


# Функция для парсинга сообщения
def parse_order_message(text):
    city_match = re.search(r'•\s*(.*?):', text)
    address_match = re.search(r'Адрес:\s*👉\s*(.*?)(?=\n|$)', text)
    count_match = re.search(r'Нужен\s*(\d+)/(\d+)', text)
    payment_match = re.search(r'Оплата:\s*(\d+)\s*₽/час', text)

    if not all([city_match, address_match, count_match, payment_match]):
        return None

    return {
        'city': city_match.group(1).strip(),
        'address': address_match.group(1).strip(),
        'body_count': int(count_match.group(2)),
        'paid_amount': int(payment_match.group(1)),
        'datetime': datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    }


async def handle_message(client: Client, message):
    if message.text:
        parsed_data = parse_order_message(message.text)
        if parsed_data:
            orders = load_orders()

            if parsed_data['city'] not in orders:
                orders[parsed_data['city']] = {}

            orders_in_address = orders[parsed_data['city']].get(parsed_data['address'], [])

            orders_in_address.append({
                'body_count': parsed_data['body_count'],
                'paid_amount': parsed_data['paid_amount'],
                'datetime': parsed_data['datetime']
            })

            orders[parsed_data['city']][parsed_data['address']] = orders_in_address

            save_orders(orders)
            print(f"Сохранена новая заявка: {parsed_data}")


# Обработка данных
def process_data(data, start_date, end_date):
    report = {}
    for city, addresses in data.items():
        address_in_price = {}
        body_in_address = {}  # кол-во людей в заявках
        city_report = {
            "unique_requests_by_price": {},
            "address_with_people": {},
            "unique_requests_count": len(addresses)
        }
        for address, orders in addresses.items():
            max_paid = 0
            for order in orders:
                order_date = datetime.strptime(order["datetime"], "%Y.%m.%d %H:%M:%S")
                if start_date <= order_date <= end_date:

                    if address not in body_in_address:
                        body_in_address[address] = [order['body_count']]
                    else:
                        body_in_address[address].append(order["body_count"])
                    # Подсчет уникальных цен по заявкам
                    max_paid = max(max_paid, order["paid_amount"])

                    '''if order['paid_amount'] in address_in_price.keys():
                        if address not in address_in_price[order['paid_amount']]:
                            address_in_price[order['paid_amount']].append(address)
                    else:
                        address_in_price[order['paid_amount']] = [address]'''
            if max_paid in city_report['unique_requests_by_price']:
                city_report['unique_requests_by_price'][max_paid] += 1
            else:
                city_report['unique_requests_by_price'][max_paid] = 1

        # считаем адреса с кол-вом заявок >= 8 или максимальным значением
        for address, body_list in body_in_address.items():
            mx_body_count = max(body_list)
            our_buddies = 0
            for b in body_list:
                if b >= 8 or b == mx_body_count:
                    our_buddies += b
            city_report['address_with_people'][address] = our_buddies

        # считаем кол-во уникальных заказов
        '''for key, value in address_in_price.items():
            address_in_price[key] = len(value)
        city_report["unique_requests_by_price"] = address_in_price'''

        report[city] = city_report

    return report


# Формирование отчета
def generate_report(report):
    report_lines = []
    for city, info in report.items():
        report_lines.append(city)
        for price, count in info["unique_requests_by_price"].items():
            report_lines.append(f" - {price} р/час ({count} заявок)")
        for address, people in info["address_with_people"].items():
            report_lines.append(f"Адрес: {address} ({people} человек)")
        report_lines.append(f"Общее число заявок ({info['unique_requests_count']})")
        report_lines.append("")
    return "\n".join(report_lines)


# Обработчик команды для получения отчета за день
@dp.message(Command("report"))
async def cmd_daily_report(message: Message):
    data = load_orders()
    now = datetime.now()
    command_args = message.text.split()[-1]
    if command_args == "day":
        start_date = now - timedelta(days=1)
    elif command_args == "week":
        start_date = now - timedelta(weeks=1)
    else:
        await message.reply("Пожалуйста, используйте команду с аргументом: /report day или /report week")
        return
    end_date = now
    report = process_data(data, start_date, end_date)
    report_text = generate_report(report)
    await message.reply(report_text)


# Обработчик команды для получения отчета за неделю
@dp.message(Command("weekly_report"))
async def cmd_weekly_report(message: Message):
    orders = load_orders()
    today = datetime.now()
    report = "Отчет за последнюю неделю:\n\n"

    for city, addresses in orders.items():
        city_orders = []
        for address, data in addresses.items():
            order_date = datetime.strptime(data['datetime'], "%Y.%m.%d %H:%M:%S")
            if today - order_date < timedelta(days=7):
                city_orders.append(
                    f"Адрес: {address}\n"
                    f"Требуется человек: {data['body_count']}\n"
                    f"Оплата: {data['paid_amount']}₽/час\n"
                    f"Время: {data['datetime']}\n"
                )

        if city_orders:
            report += f"🏙 {city}:\n" + "\n".join(city_orders) + "\n"

    if report == "Отчет за последнюю неделю:\n\n":
        report = "За последнюю неделю заявок не было."

    await message.answer(report)


# Запуск бота
async def main():
    try:
        # Инициализируем клиентов при запуске
        accounts = load_accounts()
        pyrogram_tasks = []
        for phone, data in accounts.items():
            pyrogram_tasks.append(init_account(phone, data))

        aiogram_task = dp.start_polling(bot)
        await asyncio.gather(*pyrogram_tasks, aiogram_task)
    except Exception:
        traceback.print_exc()

    finally:
        # Отключаем все клиенты при завершении работы
        for client in pyrogram_clients.values():
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
