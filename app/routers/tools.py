import json
from typing import Optional
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

        # 1) Tìm tháng mới nhất
        latest = await conn.fetchval(
            "SELECT MAX(month) FROM price_history_monthly"
        )
        if not latest:
            raise HTTPException(500, "Không có dữ liệu giá")

        # 2) Query giá trung vị theo quận
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

        # 3) Tính diện tích tối đa mua được & lọc
        results = []
        for r in rows:
            median = float(r["median_price_per_m2"])
            if median <= 0:
                continue
            max_area = req.budget_vnd / median
            if max_area < req.min_area:
                continue

            # Ước tính giá cho diện tích min_area
            price_for_min = median * req.min_area
            # Khoảng giá ±10%
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

        # 4) Sắp xếp: ưu tiên quận nhiều tin rao (dễ tìm nhà)
        results.sort(key=lambda x: x["listing_count"], reverse=True)

        return {
            "budget_vnd": req.budget_vnd,
            "min_area": req.min_area,
            "city_filter": req.city,
            "data_month": str(latest),
            "matched_districts": len(results),
            "districts": results[:15],
        }

# ── Tool Usage Log ─────────────────────────────────────────

class ToolLogRequest(BaseModel):
    tool_name: str = Field(..., max_length=50)
    session_id: Optional[str] = Field(None, max_length=100)
    input_data: Optional[dict] = Field(None)
    result_data: Optional[dict] = Field(None)
    source: str = Field("direct", max_length=20)
    city: Optional[str] = Field(None, max_length=50)
    district: Optional[str] = Field(None, max_length=50)

@router.post("/log")
async def log_tool_usage(req: ToolLogRequest, request: Request):
    pool = get_pool()
    ip = request.headers.get("x-forwarded-for", request.client.host)
    if ip and "," in ip:
        ip = ip.split(",")[0].strip()

    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO tool_usage_log
                (tool_name, session_id, input_data, result_data, source, city, district, ip_address)
            VALUES ($1, $2, $3::jsonb, $4::jsonb, $5, $6, $7, $8)
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

    return {"status": "ok"}
