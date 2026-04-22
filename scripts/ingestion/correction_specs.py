"""Correction feed contracts used by ingestion and raw loading."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FeedSpec:
    entity_name: str
    file_name: str
    headers: list[str]


CUSTOMER_FEED = FeedSpec(
    entity_name="customer_profile_changes",
    file_name="customer_profile_changes.csv.gz",
    headers=[
        "customer_unique_id",
        "effective_at",
        "customer_zip_code_prefix",
        "customer_city",
        "customer_state",
        "change_reason",
    ],
)

PRODUCT_FEED = FeedSpec(
    entity_name="product_attribute_changes",
    file_name="product_attribute_changes.csv.gz",
    headers=[
        "product_id",
        "effective_at",
        "product_category_name",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm",
        "change_reason",
    ],
)

CORRECTION_FEEDS = [CUSTOMER_FEED, PRODUCT_FEED]
