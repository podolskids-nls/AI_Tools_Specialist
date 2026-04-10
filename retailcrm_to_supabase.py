import os
import json
import requests
from dotenv import load_dotenv

# ──────────────────────────────────────────────
# НАСТРОЙКИ — берутся из .env
# ──────────────────────────────────────────────
load_dotenv()

RETAILCRM_URL = os.getenv("RETAILCRM_URL")
RETAILCRM_KEY = os.getenv("RETAILCRM_KEY")

SUPABASE_URL  = os.getenv("SUPABASE_URL")
SUPABASE_KEY  = os.getenv("SUPABASE_KEY")
# ──────────────────────────────────────────────


def fetch_orders_from_retailcrm() -> list:
    """Забирает все заказы из RetailCRM постранично."""
    orders = []
    page = 1

    while True:
        response = requests.get(
            f"{RETAILCRM_URL}/api/v5/orders",
            params={
                "apiKey": RETAILCRM_KEY,
                "limit":  50,
                "page":   page,
            },
            timeout=30,
        )
        data = response.json()

        if not data.get("success"):
            print(f"❌ Ошибка RetailCRM: {data.get('errorMsg')}")
            break

        batch = data.get("orders", [])
        orders.extend(batch)
        print(f"  Страница {page}: получено {len(batch)} заказов")

        total_pages = data.get("pagination", {}).get("totalPageCount", 1)
        if page >= total_pages:
            break
        page += 1

    return orders


def map_order(o: dict) -> dict:
    """Маппинг полей RetailCRM → таблица orders."""
    delivery = o.get("delivery", {})
    address  = delivery.get("address", {})
    custom   = {}
    for f in o.get("customFields", []):
        if isinstance(f, dict) and "code" in f:
            custom[f["code"]] = f.get("value")
    return {
        "external_id":   o.get("externalId"),
        "first_name":    o.get("firstName"),
        "last_name":     o.get("lastName"),
        "phone":         o.get("phone"),
        "email":         o.get("email"),
        "status":        o.get("status"),
        "order_method":  o.get("orderMethod"),
        "utm_source":    custom.get("utm_source"),
        "city":          address.get("city"),
        "address":       address.get("text"),
    }


def map_items(order_id: int, o: dict) -> list:
    """Маппинг товаров → таблица order_items."""
    return [
        {
            "order_id":     order_id,
            "product_name": item.get("offer", {}).get("displayName") or item.get("productName"),
            "quantity":     item.get("quantity"),
            "initial_price": item.get("initialPrice"),
        }
        for item in o.get("items", [])
    ]


def supabase_insert(table: str, rows: list) -> dict:
    """Вставка строк в таблицу Supabase через REST API."""
    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/{table}",
        headers={
            "apikey":        SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type":  "application/json",
            "Prefer":        "return=representation",
        },
        data=json.dumps(rows, ensure_ascii=False),
        timeout=30,
    )
    return response


def main():
    # 1. Забираем заказы из RetailCRM
    print("Забираем заказы из RetailCRM...")
    crm_orders = fetch_orders_from_retailcrm()
    print(f"Всего получено: {len(crm_orders)} заказов\n")

    # 2. Загружаем в Supabase
    print("Загружаем в Supabase...")
    total_orders = 0
    total_items  = 0

    for o in crm_orders:
        # Вставляем заказ
        order_row = map_order(o)
        resp = supabase_insert("orders", [order_row])

        if resp.status_code not in (200, 201):
            print(f"  ❌ Ошибка заказа {order_row['external_id']}: {resp.text}")
            continue

        inserted = resp.json()
        order_id = inserted[0]["id"]
        total_orders += 1

        # Вставляем товары заказа
        items = map_items(order_id, o)
        if items:
            resp_items = supabase_insert("order_items", items)
            if resp_items.status_code not in (200, 201):
                print(f"  ❌ Ошибка товаров заказа {order_row['external_id']}: {resp_items.text}")
            else:
                total_items += len(items)

        print(f"  ✅ {order_row['external_id']} — {len(items)} товар(ов)")

    print(f"\nГотово! Заказов: {total_orders}, товаров: {total_items}")


if __name__ == "__main__":
    main()
