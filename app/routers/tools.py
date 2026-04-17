import json
from typing import Optional
from datetime import date
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from app.db.database import get_pool

router = APIRouter(prefix="/v1/tools", tags=["tools"])

# ── Affordability ──────────────────────────────────────────

class AffordabilityRequest(BaseModel):
    budget_vnd: float = Field(..., gt=0, description="Ngân sách tối đa (VNĐ)")
    city: str = Field(None, description="Lọc theo thành phố (optional)")
    min_area: float = Field(45.0, gt=10, le=300, description="Diện tích tối thiểu m²")
    bedrooms: str = Field(None, description="Số phòng ngủ (optional)")

@router.post("/affordability")
async def affordability(req: AffordabilityRequest):
    pool = get_pool()
    async with pool.acquire() as conn:
        latest = await conn.fetchval(
            "SELECT MAX(month) FROM price_history_monthly"
        )
        if not latest:
            raise HTTPException(500, "Không có dữ liệu giá")

        if req.city:
            rows = await conn.fetch(
                """
                SELECT district, city, median_price_per_m2, avg_price_per_m2, listing_count
                FROM price_history_monthly
                WHERE month = $1
                  AND bedrooms IS NULL
                  AND property_type = 'Căn hộ'
                  AND city = $2
                  AND district != ''
                  AND median_price_per_m2 > 0
                ORDER BY median_price_per_m2
                """,
                latest, req.city
            )
        else:
            rows = await conn.fetch(
                """
                SELECT district, city, median_price_per_m2, avg_price_per_m2, listing_count
                FROM price_history_monthly
                WHERE month = $1
                  AND bedrooms IS NULL
                  AND property_type = 'Căn hộ'
                  AND district != ''
                  AND median_price_per_m2 > 0
                ORDER BY median_price_per_m2
                """,
                latest
            )

        results = []
        for r in rows:
            median = float(r["median_price_per_m2"])
            if median <= 0:
                continue
            max_area = req.budget_vnd / median
            if max_area < req.min_area:
                continue
            price_for_min = median * req.min_area
            est_min = round(price_for_min * 0.9)
            est_max = round(price_for_min * 1.1)
            results.append({
                "district": r["district"],
                "city": r["city"],
                "median_price_m2": round(median),
                "listing_count": r["listing_count"],
                "max_area_m2": round(max_area, 1),
                "price_for_min_area": round(price_for_min),
                "estimated_min": est_min,
                "estimated_max": est_max,
            })

        results.sort(key=lambda x: x["listing_count"], reverse=True)

        return {
            "budget_vnd": req.budget_vnd,
            "min_area": req.min_area,
            "city_filter": req.city,
            "data_month": str(latest),
            "matched_districts": len(results),
            "districts": results[:15],
        }


# ── Tool Usage Log + Signal Extraction ─────────────────────

class ToolLogRequest(BaseModel):
    tool_name: str = Field(..., max_length=50)
    session_id: Optional[str] = Field(None, max_length=100)
    input_data: Optional[dict] = Field(None)
    result_data: Optional[dict] = Field(None)
    source: str = Field("direct", max_length=20)
    city: Optional[str] = Field(None, max_length=50)
    district: Optional[str] = Field(None, max_length=50)


# ── Validation & price bounds ──
MIN_PRICE = 100_000_000        # 100 triệu
MAX_PRICE = 500_000_000_000    # 500 tỷ


def _valid_price(v):
    """Return True if v is a number within realistic range."""
    if v is None:
        return False
    try:
        n = float(v)
        return MIN_PRICE <= n <= MAX_PRICE
    except (TypeError, ValueError):
        return False


def _month_bucket():
    """First day of current month."""
    today = date.today()
    return date(today.year, today.month, 1)


# ── Signal extraction rules per tool ──

def _extract_affordability(inp, out, city, district, source, session_id):
    """buyer_max signal from Khả năng mua nhà."""
    price = (out or {}).get("max_price")
    if not _valid_price(price):
        return None

    inp = inp or {}
    has_loc = bool(city)
    has_prop = False  # tool này không có property detail
    has_fin = bool(inp.get("income"))

    drop = []
    if not city:
        drop.append("city")
    if not district:
        drop.append("district")
    if not inp.get("income"):
        drop.append("monthly_income")

    if has_loc and has_fin:
        comp = "full"
    elif has_loc or has_fin:
        comp = "partial"
    else:
        comp = "minimal"

    return {
        "signal_type": "buyer_max",
        "tool_source": "affordability",
        "city": city,
        "district": district,
        "project_name": None,
        "property_type": "Căn hộ",
        "bedrooms": None,
        "area_m2": None,
        "price_vnd": round(float(price)),
        "price_per_m2": None,
        "loan_ratio": None,
        "dti_ratio": float(inp["dti_max"]) * 100 if inp.get("dti_max") else None,
        "monthly_income": round(float(inp["income"])) if inp.get("income") else None,
        "confidence_score": None,
        "completeness": comp,
        "has_location": has_loc,
        "has_property_detail": has_prop,
        "has_financial_context": has_fin,
        "drop_fields": drop,
        "source": source,
        "session_id": session_id,
        "month_bucket": _month_bucket(),
    }


def _extract_post_sale(inp, out, city, district, source, session_id):
    """seller_ask signal from Thu nhập sau bán."""
    price = (inp or {}).get("sale_price")
    if not _valid_price(price):
        return None

    inp = inp or {}
    has_loc = bool(city or district)
    has_prop = bool(inp.get("area") or inp.get("bedrooms"))
    has_fin = bool(inp.get("purchase_price"))

    drop = []
    if not city:
        drop.append("city")
    if not district:
        drop.append("district")
    if not inp.get("project"):
        drop.append("project_name")
    if not inp.get("area"):
        drop.append("area_m2")
    if not inp.get("bedrooms"):
        drop.append("bedrooms")

    area = float(inp["area"]) if inp.get("area") else None
    price_f = round(float(price))

    if has_loc and has_prop and has_fin:
        comp = "full"
    elif has_loc or has_prop:
        comp = "partial"
    else:
        comp = "minimal"

    return {
        "signal_type": "seller_ask",
        "tool_source": "post_sale",
        "city": city,
        "district": district,
        "project_name": inp.get("project"),
        "property_type": "Căn hộ",
        "bedrooms": int(inp["bedrooms"]) if inp.get("bedrooms") else None,
        "area_m2": area,
        "price_vnd": price_f,
        "price_per_m2": round(price_f / area) if area and area > 0 else None,
        "loan_ratio": None,
        "dti_ratio": None,
        "monthly_income": None,
        "confidence_score": None,
        "completeness": comp,
        "has_location": has_loc,
        "has_property_detail": has_prop,
        "has_financial_context": has_fin,
        "drop_fields": drop,
        "source": source,
        "session_id": session_id,
        "month_bucket": _month_bucket(),
    }


def _extract_total_cost(inp, out, city, district, source, session_id):
    """cost_willing signal from Tổng chi phí thực."""
    price = (inp or {}).get("property_price")
    if not _valid_price(price):
        return None

    inp = inp or {}
    has_loc = bool(city or district)
    has_prop = bool(inp.get("area") or inp.get("bedrooms"))
    has_fin = bool(inp.get("loan_ratio"))

    drop = []
    if not city:
        drop.append("city")
    if not district:
        drop.append("district")
    if not inp.get("area"):
        drop.append("area_m2")
    if not inp.get("loan_ratio"):
        drop.append("loan_ratio")

    area = float(inp["area"]) if inp.get("area") else None
    price_f = round(float(price))

    if has_loc and has_prop and has_fin:
        comp = "full"
    elif has_loc or has_fin:
        comp = "partial"
    else:
        comp = "minimal"

    return {
        "signal_type": "cost_willing",
        "tool_source": "total_cost",
        "city": city,
        "district": district,
        "project_name": inp.get("project"),
        "property_type": "Căn hộ",
        "bedrooms": int(inp["bedrooms"]) if inp.get("bedrooms") else None,
        "area_m2": area,
        "price_vnd": price_f,
        "price_per_m2": round(price_f / area) if area and area > 0 else None,
        "loan_ratio": float(inp["loan_ratio"]) if inp.get("loan_ratio") else None,
        "dti_ratio": None,
        "monthly_income": None,
        "confidence_score": None,
        "completeness": comp,
        "has_location": has_loc,
        "has_property_detail": has_prop,
        "has_financial_context": has_fin,
        "drop_fields": drop,
        "source": source,
        "session_id": session_id,
        "month_bucket": _month_bucket(),
    }


def _extract_loan_calc(inp, out, city, district, source, session_id):
    """loan_capacity signal from Tính lãi vay."""
    price = (inp or {}).get("asset_value")
    if not _valid_price(price):
        return None

    inp = inp or {}
    loan_amt = float(inp["loan_amount"]) if inp.get("loan_amount") else None
    price_f = round(float(price))
    ratio = round(loan_amt / price_f * 100, 1) if loan_amt and price_f > 0 else None

    has_loc = bool(city)
    has_prop = False
    has_fin = bool(loan_amt)

    drop = []
    if not city:
        drop.append("city")
    if not district:
        drop.append("district")

    if has_loc and has_fin:
        comp = "full"
    elif has_fin:
        comp = "partial"
    else:
        comp = "minimal"

    return {
        "signal_type": "loan_capacity",
        "tool_source": "loan_calc",
        "city": city,
        "district": district,
        "project_name": None,
        "property_type": "Căn hộ",
        "bedrooms": None,
        "area_m2": None,
        "price_vnd": price_f,
        "price_per_m2": None,
        "loan_ratio": ratio,
        "dti_ratio": None,
        "monthly_income": None,
        "confidence_score": None,
        "completeness": comp,
        "has_location": has_loc,
        "has_property_detail": has_prop,
        "has_financial_context": has_fin,
        "drop_fields": drop,
        "source": source,
        "session_id": session_id,
        "month_bucket": _month_bucket(),
    }


# ── Dispatcher ──
EXTRACTORS = {
    "affordability": _extract_affordability,
    "post_sale": _extract_post_sale,
    "total_cost": _extract_total_cost,
    "loan_calculator": _extract_loan_calc,
}


async def _save_signal(conn, log_id, signal):
    """Insert one signal row into expectation_signals."""
    await conn.execute(
        """
        INSERT INTO expectation_signals
            (log_id, signal_type, tool_source, city, district, project_name,
             property_type, bedrooms, area_m2, price_vnd, price_per_m2,
             loan_ratio, dti_ratio, monthly_income, confidence_score,
             completeness, has_location, has_property_detail, has_financial_context,
             drop_fields, source, session_id, month_bucket)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23)
        """,
        log_id,
        signal["signal_type"],
        signal["tool_source"],
        signal["city"],
        signal["district"],
        signal["project_name"],
        signal["property_type"],
        signal["bedrooms"],
        signal["area_m2"],
        signal["price_vnd"],
        signal["price_per_m2"],
        signal["loan_ratio"],
        signal["dti_ratio"],
        signal["monthly_income"],
        signal["confidence_score"],
        signal["completeness"],
        signal["has_location"],
        signal["has_property_detail"],
        signal["has_financial_context"],
        signal["drop_fields"],
        signal["source"],
        signal["session_id"],
        signal["month_bucket"],
    )


# ── Main log endpoint ──────────────────────────────────────

@router.post("/log")
async def log_tool_usage(req: ToolLogRequest, request: Request):
    pool = get_pool()
    ip = request.headers.get("x-forwarded-for", request.client.host)
    if ip and "," in ip:
        ip = ip.split(",")[0].strip()

    async with pool.acquire() as conn:
        # Tầng 1: raw log
        log_id = await conn.fetchval(
            """
            INSERT INTO tool_usage_log
                (tool_name, session_id, input_data, result_data, source, city, district, ip_address)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8)
            RETURNING id
            """,
            req.tool_name,
            req.session_id,
            json.dumps(req.input_data) if req.input_data else None,
            json.dumps(req.result_data) if req.result_data else None,
            req.source,
            req.city,
            req.district,
            ip
        )

        # Tầng 2: extract signal (nếu tool có extractor)
        extractor = EXTRACTORS.get(req.tool_name)
        if extractor:
            try:
                signal = extractor(
                    req.input_data, req.result_data,
                    req.city, req.district,
                    req.source, req.session_id
                )
                if signal:
                    await _save_signal(conn, log_id, signal)
            except Exception:
                pass  # signal extraction fails silently, raw log is safe

    return {"status": "ok", "log_id": log_id}
