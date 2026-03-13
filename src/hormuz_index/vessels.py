from __future__ import annotations


def is_merchant_ship(ship_type: int | None) -> bool:
    if ship_type is None:
        return True
    excluded = {
        30, 34, 35, 36, 37,
        50, 51, 54, 55, 58,
    }
    if ship_type in excluded:
        return False
    if 60 <= ship_type <= 89:
        return True
    if ship_type in {31, 32, 33, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 52, 53, 59}:
        return True
    return ship_type >= 20
