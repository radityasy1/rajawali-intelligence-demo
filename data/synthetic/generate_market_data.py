"""
Market Data Generator for Rajawali Intelligence Demo.

Generates synthetic ISP product and provider data for the portfolio demo.
Uses realistic distributions from synthetic_config.py to create believable
fictional data that can be publicly shared.
"""

import random
from datetime import datetime, timedelta

try:
    from .synthetic_config import (
        PROVIDERS,
        SPEED_TIERS,
        PRICE_BY_SPEED,
        LOCATIONS,
        PRODUCT_NAMES,
        GIMMICKS,
        SOURCES,
        NUM_PRODUCT_RECORDS,
        NUM_PROVIDER_RECORDS,
    )
except ImportError:
    from synthetic_config import (
        PROVIDERS,
        SPEED_TIERS,
        PRICE_BY_SPEED,
        LOCATIONS,
        PRODUCT_NAMES,
        GIMMICKS,
        SOURCES,
        NUM_PRODUCT_RECORDS,
        NUM_PROVIDER_RECORDS,
    )


def weighted_choice(weighted_dict):
    """
    Select a key based on probability weights.

    Args:
        weighted_dict: Dictionary mapping items to their probability weights.
                       Weights should sum to approximately 1.0.

    Returns:
        A randomly selected key, weighted by the values.

    Example:
        >>> weighted_choice({"Jakarta": 0.30, "Surabaya": 0.15})
        "Jakarta"  # 30% of the time
    """
    items = list(weighted_dict.keys())
    weights = list(weighted_dict.values())

    # Normalize weights to sum to 1.0
    total = sum(weights)
    normalized = [w / total for w in weights]

    return random.choices(items, weights=normalized, k=1)[0]


def _generate_random_date(days_back=90):
    """
    Generate a random date within the specified number of days back.

    Args:
        days_back: Maximum number of days in the past (default 90).

    Returns:
        A date string in 'YYYY-MM-DD' format.
    """
    today = datetime.now()
    random_days = random.randint(0, days_back)
    random_date = today - timedelta(days=random_days)
    return random_date.strftime("%Y-%m-%d")


def _generate_product_name(provider, speed):
    """
    Generate a product name for a given provider and speed.

    Args:
        provider: The ISP provider name.
        speed: The speed tier in Mbps.

    Returns:
        A product name string.
    """
    templates = PRODUCT_NAMES.get(provider, ["{provider} {speed}Mbps"])
    template = random.choice(templates)
    return template.format(speed=speed, provider=provider)


def _generate_price(speed):
    """
    Generate a realistic price for a given speed tier.

    Args:
        speed: The speed tier in Mbps.

    Returns:
        Price in IDR (thousands).
    """
    min_price, max_price = PRICE_BY_SPEED.get(speed, (100, 500))
    # Round to nearest 25 for realistic pricing
    price = random.randint(min_price, max_price)
    return round(price / 25) * 25


def generate_product_detail_record(record_id):
    """
    Generate a single product detail record for dashboard_product_detail table.

    Args:
        record_id: Unique identifier for the record.

    Returns:
        Dictionary containing all fields for a product detail record:
        - id: Record identifier
        - provider: ISP provider name
        - product_name: Name of the product
        - speed_mbps: Speed tier in Mbps
        - price: Price in IDR (thousands)
        - price_unit: Currency/unit label
        - locations: Service location
        - source: Data source
        - EventDate: Record date
    """
    provider = weighted_choice(PROVIDERS)
    speed = weighted_choice(SPEED_TIERS)
    product_name = _generate_product_name(provider, speed)
    price = _generate_price(speed)
    location = weighted_choice(LOCATIONS)

    source = random.choice(SOURCES)
    date = _generate_random_date()

    return {
        "id": record_id,
        "provider": provider,
        "product_name": product_name,
        "speed_mbps": speed,
        "price": price,
        "price_unit": "IDR",
        "locations": location,
        "source": source,
        "EventDate": date,
    }


def generate_provider_matpro_record(record_id):
    """
    Generate a single provider matpro record for dashboard_provider_matpro table.

    This creates provider-level metrics combining multiple product offerings.

    Args:
        record_id: Unique identifier for the record.

    Returns:
        Dictionary containing all fields for a provider matpro record:
        - id: Record identifier
        - provider: ISP provider name
        - package_name: Product/package label
        - speed: Average speed across products
        - price: Representative price point
        - found: Primary service location
        - gimmicks: Promotional hook
        - source: Data source
        - timestamp: Record date
    """
    provider = weighted_choice(PROVIDERS)
    location = weighted_choice(LOCATIONS)

    # Generate realistic provider-level metrics
    base_market_share = PROVIDERS.get(provider, 0.10) * 100
    # Add some variance (+/- 5%)
    market_share = max(1, min(50, base_market_share + random.uniform(-5, 5)))
    market_share = round(market_share, 1)

    # Provider speed distribution (weighted by their offerings)
    avg_speed = weighted_choice(SPEED_TIERS)
    # Add variance for average
    avg_speed = round(avg_speed + random.uniform(-5, 10), 1)
    avg_speed = max(10, min(200, avg_speed))  # Clamp to valid range

    # Price range based on provider's typical offerings
    speed_keys = list(PRICE_BY_SPEED.keys())
    min_speed = min(speed_keys)
    max_speed = max(speed_keys)
    min_price = PRICE_BY_SPEED[min_speed][0]
    max_price = PRICE_BY_SPEED[max_speed][1]
    # Add variance
    min_price = max(100, min_price + random.randint(-20, 20))
    max_price = min(2000, max_price + random.randint(-100, 100))
    price = round(((min_price + max_price) / 2) / 25) * 25

    # Product count (larger providers have more products)
    base_count = int(PROVIDERS.get(provider, 0.10) * 100)
    product_count = max(1, base_count + random.randint(-5, 15))

    source = random.choice(SOURCES)
    date = _generate_random_date()
    gimmick_parts = []
    if product_count >= 20:
        gimmick_parts.append(f"{product_count} packages")
    gimmick_parts.append(f"market share {market_share}%")
    gimmick_parts.append(random.choice(GIMMICKS).lower())
    package_name = _generate_product_name(provider, int(round(avg_speed)))

    return {
        "id": record_id,
        "provider": provider,
        "package_name": package_name,
        "speed": avg_speed,
        "price": price,
        "found": location,
        "gimmicks": ", ".join(gimmick_parts),
        "source": source,
        "timestamp": f"{date} 00:00:00",
    }


def generate_all_product_details(count=None):
    """
    Generate all product detail records.

    Args:
        count: Number of records to generate. Defaults to NUM_PRODUCT_RECORDS from config.

    Returns:
        List of product detail record dictionaries.
    """
    if count is None:
        count = NUM_PRODUCT_RECORDS

    return [generate_product_detail_record(i + 1) for i in range(count)]


def generate_all_provider_matpro(count=None):
    """
    Generate all provider matpro records.

    Args:
        count: Number of records to generate. Defaults to NUM_PROVIDER_RECORDS from config.

    Returns:
        List of provider matpro record dictionaries.
    """
    if count is None:
        count = NUM_PROVIDER_RECORDS

    return [generate_provider_matpro_record(i + 1) for i in range(count)]


def to_sql_values(record):
    """
    Convert a record dictionary to SQL VALUES format.

    Handles proper escaping of strings and NULL values.

    Args:
        record: Dictionary containing record fields.

    Returns:
        String representation suitable for SQL VALUES clause.
    """
    values = []
    for key, value in record.items():
        if value is None:
            values.append("NULL")
        elif isinstance(value, str):
            # Escape single quotes
            escaped = value.replace("'", "''")
            values.append(f"'{escaped}'")
        elif isinstance(value, float):
            values.append(str(value))
        else:
            values.append(str(value))

    return f"({', '.join(values)})"


def generate_sql_inserts():
    """
    Generate SQL INSERT statements for all synthetic data.

    Creates INSERT statements for both dashboard_product_detail and
    dashboard_provider_matpro tables.

    Returns:
        Dictionary with table names as keys and SQL INSERT strings as values.
    """
    # Generate all records
    product_records = generate_all_product_details()
    provider_records = generate_all_provider_matpro()

    # Product detail INSERT
    product_columns = [
        "id", "provider", "product_name", "speed_mbps", "price",
        "price_unit", "locations", "source", "EventDate"
    ]
    product_values = [to_sql_values(r) for r in product_records]

    product_sql = f"""-- Product Detail Records
INSERT INTO dashboard_product_detail ({', '.join(product_columns)}) VALUES
{',\n'.join(product_values)};
"""

    # Provider matpro INSERT
    provider_columns = [
        "id", "provider", "package_name", "speed", "price",
        "found", "gimmicks", "source", "timestamp"
    ]
    provider_values = [to_sql_values(r) for r in provider_records]

    provider_sql = f"""-- Provider Matpro Records
INSERT INTO dashboard_provider_matpro ({', '.join(provider_columns)}) VALUES
{',\n'.join(provider_values)};
"""

    return {
        "dashboard_product_detail": product_sql,
        "dashboard_provider_matpro": provider_sql,
    }


def main():
    """
    Main entry point for command-line usage.

    Prints sample records and SQL statements to stdout.
    """
    print("=" * 60)
    print("Market Data Generator - Sample Output")
    print("=" * 60)

    # Generate sample records
    print("\n--- Sample Product Detail Records (5 samples) ---")
    for i in range(5):
        record = generate_product_detail_record(i + 1)
        print(f"\nRecord {i + 1}:")
        for key, value in record.items():
            print(f"  {key}: {value}")

    print("\n--- Sample Provider Matpro Records (5 samples) ---")
    for i in range(5):
        record = generate_provider_matpro_record(i + 1)
        print(f"\nRecord {i + 1}:")
        for key, value in record.items():
            print(f"  {key}: {value}")

    # Generate full SQL inserts
    print("\n" + "=" * 60)
    print("SQL Insert Statements (first 10 records per table)")
    print("=" * 60)

    # Generate limited sample for display
    sample_products = generate_all_product_details(10)
    sample_providers = generate_all_provider_matpro(10)

    print("\n--- Product Detail SQL ---")
    product_columns = [
        "id", "provider", "product_name", "speed_mbps", "price",
        "price_unit", "locations", "source", "EventDate"
    ]
    for record in sample_products[:3]:
        print(to_sql_values(record))

    print("\n--- Provider Matpro SQL ---")
    provider_columns = [
        "id", "provider", "package_name", "speed", "price",
        "found", "gimmicks", "source", "timestamp"
    ]
    for record in sample_providers[:3]:
        print(to_sql_values(record))

    # Summary stats
    print("\n" + "=" * 60)
    print("Generation Summary")
    print("=" * 60)
    print(f"Total product records: {NUM_PRODUCT_RECORDS}")
    print(f"Total provider records: {NUM_PROVIDER_RECORDS}")
    print(f"Providers: {list(PROVIDERS.keys())}")
    print(f"Speed tiers: {list(SPEED_TIERS.keys())} Mbps")
    print(f"Locations: {list(LOCATIONS.keys())}")


if __name__ == "__main__":
    main()
