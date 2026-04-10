import os
import json
import requests
import sys
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# НАСТРОЙКИ — берутся из .env
# ──────────────────────────────────────────────
load_dotenv()

API_URL = os.getenv("RETAILCRM_URL")   # без слэша в конце
API_KEY = os.getenv("RETAILCRM_KEY")
SITE    = os.getenv("RETAILCRM_SITE")  # Настройки → Магазины → код магазина
# ──────────────────────────────────────────────

ORDERS_FILE = "mock_orders.json"
UPLOAD_URL  = f"{API_URL}/api/v5/orders/upload"
BATCH_SIZE  = 50   # максимум по документации RetailCRM


def load_orders(path: str) -> list:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def add_external_ids(orders: list) -> list:
    """
    externalId обязателен для /orders/upload.
    Генерируем его из порядкового номера, чтобы повторный запуск
    не дублировал заказы (RetailCRM вернёт ошибку на уже существующий id).
    """
    for i, order in enumerate(orders, start=1):
        if "externalId" not in order:
            order["externalId"] = f"mock-{i}"
    return orders


def upload_batch(batch: list) -> dict:
    for o in batch:
        o["orderType"] = "main"
    response = requests.post(
        UPLOAD_URL,
        data={
            "apiKey": API_KEY,
            "site":   SITE,
            "orders": json.dumps(batch, ensure_ascii=False),
        },
        timeout=30,
    )
    return response.json()


def main():
    print(f"Загружjаем заказы из {ORDERS_FILE}...")
    orders = load_orders(ORDERS_FILE)
    orders = add_external_ids(orders)
    total  = len(orders)
    print(f"Найдено заказов: {total}")

    uploaded = 0
    errors   = []

    for start in range(0, total, BATCH_SIZE):
        batch = orders[start : start + BATCH_SIZE]
        result = upload_batch(batch)

        if result.get("success"):
            uploaded += len(batch)
            print(f"  ✅ Загружено {uploaded}/{total}")
        else:
            msg = result.get("errorMsg", "неизвестная ошибка")
            errs = result.get("errors", [])
            print(f"  ❌ Ошибка: {msg}")
            if errs:
                for e in errs:
                    print(f"     • {e}")
            errors.append(result)

    print()
    if not errors:
        print(f"Готово! Все {uploaded} заказов успешно загружены.")
    else:
        print(f"Загружено: {uploaded}/{total}. Пакетов с ошибками: {len(errors)}.")
        sys.exit(1)


if __name__ == "__main__":
    main()
