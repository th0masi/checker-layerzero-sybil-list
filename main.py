import pandas as pd
import aiosqlite
import aiofiles
import sys
import os
import asyncio
from loguru import logger
from InquirerPy import inquirer
from tabulate import tabulate
from termcolor import colored


def setup_logger():
    logger.remove()

    format_str = ("<green>{time:DD.MM HH:mm:ss}</green> | "
                  "<level>{message}</level>")

    logger.add(
        sys.stdout,
        colorize=True,
        format=format_str,
        level="INFO"
    )

    logger.add(
        'logfile.log',
        format="{time:DD.MM HH:mm:ss} | {name} - {message}",
        level="INFO",
        encoding='utf-8',
        errors='ignore'
    )


async def read_wallets_from_file(wallets_file):
    wallets = set()
    async with aiofiles.open(wallets_file, 'r') as file:
        async for line in file:
            address = line.strip().lower()
            if address:
                wallets.add(address)

    if not wallets:
        logger.error("Файл wallets.txt пуст или содержит некорректные данные")
        return None

    return wallets


async def database_exists(db_path):
    try:
        async with aiosqlite.connect(db_path) as db:
            async with db.execute('''
                SELECT name 
                FROM sqlite_master 
                WHERE type='table' AND name='data_table'
            ''') as cursor:
                table = await cursor.fetchone()
                if table is not None:
                    # Проверяем, есть ли данные в таблице
                    async with db.execute('SELECT COUNT(*) FROM data_table') as count_cursor:
                        count = await count_cursor.fetchone()
                        return count[0] > 0
                return False
    except Exception as e:
        logger.error(f"Ошибка при проверке базы данных: {e}")
        return False


async def create_database_from_csv(csv_file, db_path):
    if await database_exists(db_path):
        logger.info("База данных уже существует и содержит данные")
        return

    df = pd.read_csv(csv_file, low_memory=False)

    expected_columns = ['Folder Name', 'File Name', 'Line']

    if not all(col in df.columns for col in expected_columns):
        raise ValueError(
            "CSV file must contain 'Folder Name', 'File Name', and 'Line' columns")

    logger.info("Создаю БД для удобного поиска...")

    async with aiosqlite.connect(db_path) as db:
        await db.execute('''
            CREATE TABLE IF NOT EXISTS data_table (
                source TEXT,
                cluster TEXT,
                address TEXT
            )
        ''')

        await db.execute('''
            CREATE TABLE IF NOT EXISTS deleted_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source TEXT NOT NULL,
                deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')

        await db.execute('DELETE FROM data_table')

        for _, row in df.iterrows():
            await db.execute('''
                INSERT INTO data_table (source, cluster, address) VALUES (?, ?, ?)
            ''', (row['Folder Name'], row['File Name'], row['Line']))

        await db.execute(
            'CREATE INDEX IF NOT EXISTS idx_address ON data_table(address)')

        await db.commit()

    logger.info("База данных создана и данные импортированы")


async def count_unique_addresses(db_path):
    async with aiosqlite.connect(db_path) as db:
        async with db.execute('SELECT COUNT(*) FROM data_table') as cursor:
            total_rows = await cursor.fetchone()
            total_rows = total_rows[0]

        async with db.execute('SELECT COUNT(*) FROM data_table WHERE address IS NOT NULL') as cursor:
            address_rows = await cursor.fetchone()
            address_rows = address_rows[0]

        async with db.execute('SELECT COUNT(DISTINCT address) FROM data_table') as cursor:
            unique_count = await cursor.fetchone()
            unique_count = unique_count[0]

        logger.info(f"Общее количество строк: {total_rows}")
        logger.info(f"Количество строк с адресами: {address_rows}")
        logger.info(f"Уникальных адресов: {unique_count}")


async def view_selected_wallet_statistics(db_path, wallets_file):
    wallets = await read_wallets_from_file(wallets_file)
    if wallets is None:
        return

    async with aiosqlite.connect(db_path) as db:
        query = '''
            SELECT source, COUNT(DISTINCT address) as wallet_count
            FROM data_table
            WHERE address IN ({})
            GROUP BY source
            ORDER BY wallet_count DESC
        '''.format(','.join('?' * len(wallets)))

        async with db.execute(query, tuple(wallets)) as cursor:
            rows = await cursor.fetchall()

    if rows:
        print(tabulate(rows, headers=["Источник", "Кол-во кошельков"], tablefmt="plain"))
    else:
        logger.error("Нет данных для отображения статистики кошельков")


async def view_wallet_statistics(db_path):
    async with aiosqlite.connect(db_path) as db:
        query = '''
            SELECT source, COUNT(address) as wallet_count
            FROM data_table
            GROUP BY source
            ORDER BY wallet_count DESC
        '''
        async with db.execute(query) as cursor:
            rows = await cursor.fetchall()

    if rows:
        print(tabulate(rows, headers=["Источник", "Кол-во кошельков"], tablefmt="plain"))
    else:
        logger.error("Нет данных для отображения статистики кошельков")


async def delete_source(db_path, source_name):
    source_name = source_name.strip()

    async with aiosqlite.connect(db_path) as db:
        await db.execute('INSERT INTO deleted_sources (source) VALUES (?)',
                         (source_name,))
        async with db.execute('DELETE FROM data_table WHERE source = ?',
                              (source_name,)) as cursor:
            deleted_rows = cursor.rowcount
        await db.commit()

    logger.info(f"Источник '{source_name}' был удален")
    logger.info(f"Удалено строк: {deleted_rows}")


async def delete_database(db_path):
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
            logger.info(f"База данных по пути '{db_path}' успешно удалена")
            logger.info(f"Перезапустите софт")
        else:
            logger.error(f"Файл базы данных по пути '{db_path}' не существует")
    except Exception as e:
        logger.error(f"Ошибка при удалении базы данных: {e}")


async def check_sybil_wallets(db_path, wallets_file):
    results = []
    total_wallets = 0
    sybil_wallets = 0
    non_sybil_wallets = 0
    wallets = await read_wallets_from_file(wallets_file)
    if wallets is None:
        return

    async with aiosqlite.connect(db_path) as db:
        for address in wallets:
            total_wallets += 1

            query = '''
                SELECT d.address, d.source
                FROM data_table d
                WHERE d.address = ?
            '''
            async with db.execute(query, (address,)) as cursor:
                sources = set()
                async for row in cursor:
                    sources.add(row[1])

                if sources:
                    sybil_wallets += 1
                    results.append(
                        (address, 'ДА', len(sources), '\n'.join(sources)))
                else:
                    non_sybil_wallets += 1
                    results.append((address, 'НЕТ', 0, ''))

    results.sort(
        key=lambda x: (x[1] == "НЕТ", x[0] if x[0] is not None else ""))

    colored_results = []
    for result in results:
        if result[1] == 'ДА':
            colored_results.append([colored(cell, 'red') for cell in result])
        else:
            colored_results.append([colored(cell, 'green') for cell in result])

    print(tabulate(colored_results,
                   headers=["Кошелек", "Сибил", "Кол-во источников", "Источники"],
                   tablefmt="plain"))

    sybil_percentage = (sybil_wallets / total_wallets) * 100 if total_wallets > 0 else 0
    sybil_percentage = round(sybil_percentage, 2)

    print(
        f"\nКошельков: {colored(total_wallets, 'white')} | "
        f"Сибильных: {colored(sybil_wallets, 'red')} ({colored(sybil_percentage, 'red')}%) | "
        f"Чистых: {colored(non_sybil_wallets, 'green')}"
    )

    df = pd.DataFrame(results, columns=["Кошелек", "Сибил", "Кол-во источников", "Источники"])
    csv_path = 'results.csv'
    df.to_csv(csv_path, index=False, encoding='utf-8-sig')

    absolute_path = os.path.abspath(csv_path)
    print(f"Данные сохранены в таблицу results.csv, путь: '{absolute_path}'")


async def view_deleted_sources(db_path):
    async with aiosqlite.connect(db_path) as db:
        async with db.execute('SELECT source, deleted_at FROM deleted_sources') as cursor:
            rows = await cursor.fetchall()

    if rows:
        print(tabulate(rows, headers=["Источник", "Удалено"], tablefmt="plain"))
    else:
        logger.info("Нет удаленных источников")


async def main_loop():
    csv_file = 'data/data.csv'
    db_path = 'data.db'
    wallets_file = 'data/wallets.txt'

    setup_logger()
    await create_database_from_csv(csv_file, db_path)
    await count_unique_addresses(db_path)

    while True:
        print()
        choice = await inquirer.select(
            message="Выберите действие:",
            choices=[
                {"name": "Проверить мои кошельки", "value": "check_sybil"},
                {"name": "Статистика моих кошельков", "value": "view_selected_wallet_statistics"},
                {"name": "Удалить источник", "value": "delete_source"},
                {"name": "Посмотреть удаленные источники", "value": "view_sources"},
                {"name": "Статистика всех кошельков", "value": "view_wallet_statistics"},
                {"name": "Удалить БД и начать заново", "value": "delete_db"},
                {"name": "Выход", "value": "exit"}
            ]
        ).execute_async()

        if choice == "check_sybil":
            await check_sybil_wallets(db_path, wallets_file)
        elif choice == "delete_source":
            source_name = await inquirer.text(
                message="Введите имя источника для удаления:"
            ).execute_async()
            if source_name:
                await delete_source(db_path, source_name)
        elif choice == "view_sources":
            await view_deleted_sources(db_path)
        elif choice == "view_wallet_statistics":
            await view_wallet_statistics(db_path)
        elif choice == "view_selected_wallet_statistics":
            await view_selected_wallet_statistics(db_path, wallets_file)
        elif choice == "delete_db":
            await delete_database(db_path)
            break
        elif choice == "exit":
            break


if __name__ == "__main__":
    asyncio.run(main_loop())
