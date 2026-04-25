"""Create the committed small Olist fixture dataset used by CI.

The fixture is synthetic but keeps the original Olist file names, headers, and
referential shape. It is intentionally tiny so CI can run the real pipeline
without depending on the full Kaggle archive.
"""

from __future__ import annotations

import csv
import json
import shutil
from decimal import Decimal
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

PROJECT_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_ROOT = PROJECT_ROOT / "tests" / "fixtures" / "olist_small"
SOURCE_DIR = FIXTURE_ROOT / "source"
ARCHIVE_PATH = FIXTURE_ROOT / "olist_small.zip"
PROFILE_PATH = FIXTURE_ROOT / "source_profile_small.json"


CUSTOMER_COLUMNS = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state",
]
GEOLOCATION_COLUMNS = [
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state",
]
ORDER_ITEM_COLUMNS = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value",
]
PAYMENT_COLUMNS = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value",
]
REVIEW_COLUMNS = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp",
]
ORDER_COLUMNS = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]
PRODUCT_COLUMNS = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm",
]
SELLER_COLUMNS = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state",
]
TRANSLATION_COLUMNS = [
    "product_category_name",
    "product_category_name_english",
]


SOURCE_FILES = [
    ("olist_customers_dataset.csv", "customers", CUSTOMER_COLUMNS),
    ("olist_geolocation_dataset.csv", "geolocation", GEOLOCATION_COLUMNS),
    ("olist_order_items_dataset.csv", "order_items", ORDER_ITEM_COLUMNS),
    ("olist_order_payments_dataset.csv", "order_payments", PAYMENT_COLUMNS),
    ("olist_order_reviews_dataset.csv", "order_reviews", REVIEW_COLUMNS),
    ("olist_orders_dataset.csv", "orders", ORDER_COLUMNS),
    ("olist_products_dataset.csv", "products", PRODUCT_COLUMNS),
    ("olist_sellers_dataset.csv", "sellers", SELLER_COLUMNS),
    (
        "product_category_name_translation.csv",
        "product_category_translation",
        TRANSLATION_COLUMNS,
    ),
]

RAW_TYPES = {
    "customer_id": "varchar(256)",
    "customer_unique_id": "varchar(256)",
    "customer_zip_code_prefix": "varchar(16)",
    "customer_city": "varchar(256)",
    "customer_state": "varchar(256)",
    "geolocation_zip_code_prefix": "varchar(16)",
    "geolocation_lat": "decimal(18, 14)",
    "geolocation_lng": "decimal(18, 14)",
    "geolocation_city": "varchar(256)",
    "geolocation_state": "varchar(256)",
    "order_id": "varchar(256)",
    "order_item_id": "integer",
    "product_id": "varchar(256)",
    "seller_id": "varchar(256)",
    "shipping_limit_date": "timestamp",
    "price": "decimal(18, 2)",
    "freight_value": "decimal(18, 2)",
    "payment_sequential": "integer",
    "payment_type": "varchar(256)",
    "payment_installments": "integer",
    "payment_value": "decimal(18, 2)",
    "review_id": "varchar(256)",
    "review_score": "integer",
    "review_comment_title": "varchar(1024)",
    "review_comment_message": "varchar(65535)",
    "review_creation_date": "timestamp",
    "review_answer_timestamp": "timestamp",
    "order_status": "varchar(256)",
    "order_purchase_timestamp": "timestamp",
    "order_approved_at": "timestamp",
    "order_delivered_carrier_date": "timestamp",
    "order_delivered_customer_date": "timestamp",
    "order_estimated_delivery_date": "timestamp",
    "product_category_name": "varchar(256)",
    "product_name_lenght": "integer",
    "product_description_lenght": "integer",
    "product_photos_qty": "integer",
    "product_weight_g": "integer",
    "product_length_cm": "integer",
    "product_height_cm": "integer",
    "product_width_cm": "integer",
    "seller_zip_code_prefix": "varchar(16)",
    "seller_city": "varchar(256)",
    "seller_state": "varchar(256)",
    "product_category_name_english": "varchar(256)",
}


def rows_by_file() -> dict[str, list[dict[str, str]]]:
    categories = [
        ("beleza_saude", "health_beauty"),
        ("informatica_acessorios", "computers_accessories"),
        ("moveis_decoracao", "furniture_decor"),
        ("telefonia", "telephony"),
        ("utilidades_domesticas", "housewares"),
    ]

    customers = [
        {
            "customer_id": f"customer_{index:03d}",
            "customer_unique_id": f"unique_{((index - 1) % 6) + 1:03d}",
            "customer_zip_code_prefix": f"010{index:02d}",
            "customer_city": ["sao paulo", "rio de janeiro", "curitiba", "salvador"][
                index % 4
            ],
            "customer_state": ["SP", "RJ", "PR", "BA"][index % 4],
        }
        for index in range(1, 9)
    ]

    geolocation = [
        {
            "geolocation_zip_code_prefix": f"010{index:02d}",
            "geolocation_lat": f"-23.{540000 + index:06d}",
            "geolocation_lng": f"-46.{630000 + index:06d}",
            "geolocation_city": ["sao paulo", "rio de janeiro", "curitiba", "salvador"][
                index % 4
            ],
            "geolocation_state": ["SP", "RJ", "PR", "BA"][index % 4],
        }
        for index in range(1, 7)
    ]

    products = [
        {
            "product_id": f"product_{index:03d}",
            "product_category_name": categories[(index - 1) % len(categories)][0],
            "product_name_lenght": str(30 + index),
            "product_description_lenght": str(120 + index * 7),
            "product_photos_qty": str((index % 4) + 1),
            "product_weight_g": str(400 + index * 45),
            "product_length_cm": str(15 + index),
            "product_height_cm": str(8 + index),
            "product_width_cm": str(12 + index),
        }
        for index in range(1, 9)
    ]

    sellers = [
        {
            "seller_id": f"seller_{index:03d}",
            "seller_zip_code_prefix": f"020{index:02d}",
            "seller_city": ["campinas", "santos", "niteroi", "curitiba"][index % 4],
            "seller_state": ["SP", "SP", "RJ", "PR"][index % 4],
        }
        for index in range(1, 5)
    ]

    orders = []
    for index in range(1, 13):
        purchase_day = index + 3
        delivered_day = purchase_day + 5 + (index % 3)
        estimated_day = purchase_day + 9
        orders.append(
            {
                "order_id": f"order_{index:03d}",
                "customer_id": customers[(index - 1) % len(customers)]["customer_id"],
                "order_status": "delivered",
                "order_purchase_timestamp": f"2018-0{((index - 1) % 6) + 1}-{purchase_day:02d} 10:15:00",
                "order_approved_at": f"2018-0{((index - 1) % 6) + 1}-{purchase_day:02d} 11:05:00",
                "order_delivered_carrier_date": f"2018-0{((index - 1) % 6) + 1}-{purchase_day + 1:02d} 09:00:00",
                "order_delivered_customer_date": f"2018-0{((index - 1) % 6) + 1}-{delivered_day:02d} 14:30:00",
                "order_estimated_delivery_date": f"2018-0{((index - 1) % 6) + 1}-{estimated_day:02d} 00:00:00",
            }
        )

    order_items = []
    for index, order in enumerate(orders, start=1):
        item_count = 2 if index in {3, 6, 9, 12} else 1
        for item_number in range(1, item_count + 1):
            product_index = ((index + item_number - 2) % len(products)) + 1
            seller_index = ((index + item_number - 2) % len(sellers)) + 1
            price = Decimal("20.00") + Decimal(index * 3) + Decimal(item_number * 2)
            freight = Decimal("5.00") + Decimal(item_number)
            order_items.append(
                {
                    "order_id": order["order_id"],
                    "order_item_id": str(item_number),
                    "product_id": f"product_{product_index:03d}",
                    "seller_id": f"seller_{seller_index:03d}",
                    "shipping_limit_date": order["order_approved_at"],
                    "price": f"{price:.2f}",
                    "freight_value": f"{freight:.2f}",
                }
            )

    payment_types = ["credit_card", "boleto", "voucher", "debit_card"]
    payments = []
    for index, order in enumerate(orders, start=1):
        order_total = sum(
            Decimal(item["price"]) + Decimal(item["freight_value"])
            for item in order_items
            if item["order_id"] == order["order_id"]
        )
        if index in {4, 8}:
            payments.append(
                {
                    "order_id": order["order_id"],
                    "payment_sequential": "1",
                    "payment_type": "voucher",
                    "payment_installments": "1",
                    "payment_value": "10.00",
                }
            )
            payments.append(
                {
                    "order_id": order["order_id"],
                    "payment_sequential": "2",
                    "payment_type": "credit_card",
                    "payment_installments": "2",
                    "payment_value": f"{order_total - Decimal('10.00'):.2f}",
                }
            )
        else:
            payments.append(
                {
                    "order_id": order["order_id"],
                    "payment_sequential": "1",
                    "payment_type": payment_types[index % len(payment_types)],
                    "payment_installments": str((index % 3) + 1),
                    "payment_value": f"{order_total:.2f}",
                }
            )

    reviews = [
        {
            "review_id": f"review_{index:03d}",
            "order_id": order["order_id"],
            "review_score": str((index % 5) + 1),
            "review_comment_title": "ok",
            "review_comment_message": "fixture review",
            "review_creation_date": order["order_delivered_customer_date"][:10]
            + " 00:00:00",
            "review_answer_timestamp": order["order_delivered_customer_date"][:10]
            + " 12:00:00",
        }
        for index, order in enumerate(orders, start=1)
    ]

    translations = [
        {
            "product_category_name": category,
            "product_category_name_english": english,
        }
        for category, english in categories
    ]

    return {
        "olist_customers_dataset.csv": customers,
        "olist_geolocation_dataset.csv": geolocation,
        "olist_order_items_dataset.csv": order_items,
        "olist_order_payments_dataset.csv": payments,
        "olist_order_reviews_dataset.csv": reviews,
        "olist_orders_dataset.csv": orders,
        "olist_products_dataset.csv": products,
        "olist_sellers_dataset.csv": sellers,
        "product_category_name_translation.csv": translations,
    }


def write_csv(path: Path, columns: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def write_archive(rows: dict[str, list[dict[str, str]]]) -> None:
    with ZipFile(ARCHIVE_PATH, "w", compression=ZIP_DEFLATED) as archive:
        for file_name, _, _ in SOURCE_FILES:
            archive.write(SOURCE_DIR / file_name, arcname=file_name)


def write_profile(rows: dict[str, list[dict[str, str]]]) -> None:
    profile = [
        {
            "file_name": file_name,
            "entity_name": entity_name,
            "row_count": len(rows[file_name]),
            "columns": [
                {
                    "name": column,
                    "redshift_raw_type": RAW_TYPES[column],
                }
                for column in columns
            ],
        }
        for file_name, entity_name, columns in SOURCE_FILES
    ]
    PROFILE_PATH.write_text(json.dumps(profile, indent=2), encoding="utf-8")


def main() -> None:
    if SOURCE_DIR.exists():
        shutil.rmtree(SOURCE_DIR)
    FIXTURE_ROOT.mkdir(parents=True, exist_ok=True)

    rows = rows_by_file()
    for file_name, _, columns in SOURCE_FILES:
        write_csv(SOURCE_DIR / file_name, columns, rows[file_name])

    write_archive(rows)
    write_profile(rows)
    print(f"Wrote {ARCHIVE_PATH}")
    print(f"Wrote {PROFILE_PATH}")


if __name__ == "__main__":
    main()
