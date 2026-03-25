"""Helpers for MeteoSwiss open data hourly forecast conditions."""

from __future__ import annotations

import csv
import datetime as dt
import io
import logging
from typing import Any

from aiohttp import ClientSession

_LOGGER = logging.getLogger(__name__)

LOCAL_FORECAST_ITEM_URL = (
    "https://data.geo.admin.ch/api/stac/v1/collections/"
    "ch.meteoschweiz.ogd-local-forecasting/items/{date}-ch"
)
LOCAL_FORECAST_META_POINT_URL = (
    "https://data.geo.admin.ch/ch.meteoschweiz.ogd-local-forecasting/"
    "ogd-local-forecasting_meta_point.csv"
)

_POINT_ID_BY_POSTCODE: dict[int, int] = {}


def _forecast_time_to_iso8601_z(value: str) -> str:
    forecast_time = dt.datetime.strptime(value, "%Y%m%d%H%M").replace(
        tzinfo=dt.timezone.utc
    )
    return forecast_time.isoformat("T").partition("+")[0] + "Z"


def _latest_jww_asset_url(item: dict[str, Any]) -> str | None:
    assets: dict[str, Any] = item.get("assets", {})
    keys = sorted(key for key in assets if key.endswith(".jww003i0.csv"))
    if not keys:
        return None
    asset = assets[keys[-1]]
    return asset.get("href")


async def _async_get_postcode_point_id(
    session: ClientSession,
    postcode: int,
) -> int | None:
    if postcode in _POINT_ID_BY_POSTCODE:
        return _POINT_ID_BY_POSTCODE[postcode]

    async with session.get(LOCAL_FORECAST_META_POINT_URL) as response:
        response.raise_for_status()
        text = await response.text(encoding="latin-1")

    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    for row in reader:
        raw_postcode = row.get("postal_code")
        raw_point_id = row.get("point_id")
        if not raw_postcode or not raw_point_id:
            continue
        try:
            row_postcode = int(raw_postcode)
            row_point_id = int(raw_point_id)
        except ValueError:
            continue
        _POINT_ID_BY_POSTCODE[row_postcode] = row_point_id

    return _POINT_ID_BY_POSTCODE.get(postcode)


async def async_fetch_hourly_condition_codes(
    session: ClientSession,
    postcode: int,
) -> dict[str, int]:
    """Fetch hourly MeteoSwiss icon codes keyed by forecast timestamp."""
    total_started = dt.datetime.now(dt.timezone.utc)
    point_lookup_started = dt.datetime.now(dt.timezone.utc)
    point_id = await _async_get_postcode_point_id(session, postcode)
    point_lookup_seconds = (
        dt.datetime.now(dt.timezone.utc) - point_lookup_started
    ).total_seconds()
    if point_id is None:
        _LOGGER.warning("No MeteoSwiss open data point found for postcode %s", postcode)
        return {}

    now_utc = dt.datetime.now(dt.timezone.utc).date()
    asset_url: str | None = None
    stac_lookup_seconds = 0.0
    for date_value in (now_utc, now_utc - dt.timedelta(days=1)):
        item_url = LOCAL_FORECAST_ITEM_URL.format(date=date_value.strftime("%Y%m%d"))
        stac_lookup_started = dt.datetime.now(dt.timezone.utc)
        async with session.get(item_url) as response:
            if response.status == 404:
                stac_lookup_seconds += (
                    dt.datetime.now(dt.timezone.utc) - stac_lookup_started
                ).total_seconds()
                continue
            response.raise_for_status()
            item = await response.json()
        stac_lookup_seconds += (
            dt.datetime.now(dt.timezone.utc) - stac_lookup_started
        ).total_seconds()
        asset_url = _latest_jww_asset_url(item)
        if asset_url:
            break

    if asset_url is None:
        _LOGGER.warning(
            "No jww003i0 asset found in MeteoSwiss open data for postcode %s",
            postcode,
        )
        return {}

    csv_fetch_started = dt.datetime.now(dt.timezone.utc)
    async with session.get(asset_url) as response:
        response.raise_for_status()
        text = await response.text(encoding="latin-1")
    csv_fetch_seconds = (
        dt.datetime.now(dt.timezone.utc) - csv_fetch_started
    ).total_seconds()

    codes: dict[str, int] = {}
    reader = csv.DictReader(io.StringIO(text), delimiter=";")
    for row in reader:
        if row.get("point_id") != str(point_id):
            continue
        raw_date = row.get("Date")
        raw_code = row.get("jww003i0")
        if not raw_date or not raw_code:
            continue
        try:
            codes[_forecast_time_to_iso8601_z(raw_date)] = int(raw_code)
        except ValueError:
            continue

    total_seconds = (dt.datetime.now(dt.timezone.utc) - total_started).total_seconds()
    _LOGGER.debug(
        "Fetched %d hourly MeteoSwiss icon codes for postcode %s from %s in %.2fs (point_lookup=%.2fs stac_lookup=%.2fs csv_fetch=%.2fs)",
        len(codes),
        postcode,
        asset_url,
        total_seconds,
        point_lookup_seconds,
        stac_lookup_seconds,
        csv_fetch_seconds,
    )
    return codes
