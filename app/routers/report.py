# app/routers/report.py
"""
Báo cáo chi tiết cho Hành trình B (Chủ nhà)
3 endpoints công khai, rate-limit 30 req/min/IP
"""

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
import time, math

router = APIRouter(prefix="/v1/report", tags=["report"])

# ============================================================
# RATE LIMITER (giống estimate.py)
# ============================================================
_rate_store = {}
RATE_LIMIT = 30
RATE_WINDOW = 60

def check_rate(ip: str):
    now = time.time()
    if ip not in _rate_store:
        _rate_store[ip] = []
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT:
        raise HTTPException(429, "Rate limit exceeded")
    _rate_store[ip].append(now)

# ============================================================
# HELPERS
# ============================================================
DISTRICT_ALIASES = {
    "quan 1": "1", "quan 2": "2", "quan 3": "3", "quan 4": "4",
    "quan 5": "5", "quan 6": "6", "quan 7": "7", "quan 8": "8",
    "quan 9": "9", "quan 10": "10", "quan 11": "11", "quan 12": "12",
    "thu duc": "Thủ Đức", "binh thanh": "Bình Thạnh",
    "go vap": "Gò Vấp", "tan binh": "Tân Bình",
    "tan phu": "Tân Phú", "phu nhuan": "Phú Nhuận",
    "binh tan": "Bình Tân", "nha be": "Nhà Bè",
    "binh chanh": "Bình Chánh", "hoc mon": "Hóc Môn",
}

CITY_ALIASES = {
    "hcm": "Hồ Chí Minh", "ho chi minh": "Hồ Chí Minh",
    "saigon": "Hồ Chí Minh", "sg": "Hồ Chí Minh",
    "ha noi": "Hà Nội", "hn": "Hà Nội",
    "binh duong": "Bình Dương", "dong nai": "Đồng Nai",
    "da nang": "Đà Nẵng", "long an": "Long An",
}

def norm(s):
    if not s:
        return ""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).lower().strip()

def resolve_district(d):
    if not d:
        return d
    key = norm(d)
    return DISTRICT_ALIASES.get(key, d.strip())

def resolve_city(c):
    if not c:
        return c
    key = norm(c)
    return CITY_ALIASES.get(key, c.strip())

def make_slug(name):
    if not name:
        return ""
    import re, unicodedata
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_str = "".join(c for c in nfkd if not unicodedata.combining(c))
    slug = re.sub(r"[^a-z0-9]+", "-", ascii_str.lower()).strip("-")
    return slug

# ============================================================
# ENDPOINT 1: PROFIT — Lời/lỗ, CAGR, yield, so sánh tiết kiệm
# ============================================================
class ProfitRequest(BaseModel):
    project_name: str = ""
    city: str = ""
    district: str = ""
    bedrooms: str = ""
    area_sqm: float = 0
    purchase_price: float = 0       # tỷ VNĐ
    purchase_year: int = 2020
    purchase_month: int = 1
    current_status: str = ""        # living / renting / vacant
    actual_rent: float = 0          # triệu/tháng
    furnishing: str = ""

@router.post("/profit")
async def report_profit(req: ProfitRequest, request: Request):
    check_rate(request.client.host)
    db = request.app.state.db

    district = resolve_district(req.district)
    city = resolve_city(req.city)
    slug = make_slug(req.project_name)
    bedrooms_int = None
    try:
        bedrooms_int = int(req.bedrooms)
    except (ValueError, TypeError):
        pass

    # --- Lấy giá thị trường (MVP trước, fallback quận) ---
    source = "none"
    estimated_min = 0
    estimated_max = 0
    avg_m2 = 0
    sale_count = 0
    rental_yield_market = 0

    # MVP check
    if slug and bedrooms_int:
        row = await db.fetchrow(
            """SELECT estimated_low, estimated_high, sale_median_m2,
                      rental_yield, sale_count
               FROM mvp_valuation
               WHERE project_slug = $1 AND bedrooms = $2""",
            slug, bedrooms_int
        )
        if row and row["estimated_low"]:
            source = "project"
            estimated_min = float(row["estimated_low"])
            estimated_max = float(row["estimated_high"])
            avg_m2 = float(row["sale_median_m2"]) if row["sale_median_m2"] else 0
            sale_count = int(row["sale_count"]) if row["sale_count"] else 0
            rental_yield_market = float(row["rental_yield"]) if row["rental_yield"] else 0

    # Fallback quận
    if source == "none" and district and city and req.area_sqm > 0:
        row = await db.fetchrow(
            """SELECT avg_price_per_m2, median_price_per_m2, listing_count
               FROM price_history_monthly
               WHERE district = $1 AND city = $2
                 AND property_type = 'Căn hộ'
               ORDER BY month DESC LIMIT 1""",
            district, city
        )
        if row and row["avg_price_per_m2"]:
            source = "district"
            avg_m2 = float(row["median_price_per_m2"] or row["avg_price_per_m2"])
            total = avg_m2 * req.area_sqm
            estimated_min = total * 0.9
            estimated_max = total * 1.1
            sale_count = int(row["listing_count"]) if row["listing_count"] else 0

    # --- Tính toán ---
    purchase_vnd = req.purchase_price * 1e9
    est_avg = (estimated_min + estimated_max) / 2 if estimated_min else 0

    # Lời/lỗ
    profit_vnd = est_avg - purchase_vnd if est_avg else 0
    profit_pct = (profit_vnd / purchase_vnd * 100) if purchase_vnd else 0

    # Số năm nắm giữ
    from datetime import date
    today = date.today()
    purchase_date = date(req.purchase_year, req.purchase_month, 1)
    days_held = (today - purchase_date).days
    years_held = days_held / 365.25 if days_held > 0 else 0.01

    # CAGR
    cagr = 0
    if est_avg and purchase_vnd and years_held > 0:
        cagr = (math.pow(est_avg / purchase_vnd, 1 / years_held) - 1) * 100

    # Rental yield thực (nếu đang cho thuê)
    actual_yield = 0
    if req.actual_rent and req.actual_rent > 0 and purchase_vnd > 0:
        actual_yield = (req.actual_rent * 12 * 1e6) / purchase_vnd * 100

    # So sánh gửi tiết kiệm (5%/năm)
    savings_rate = 5.0
    savings_result = purchase_vnd * math.pow(1 + savings_rate / 100, years_held)
    savings_profit = savings_result - purchase_vnd
    savings_profit_pct = (savings_profit / purchase_vnd * 100) if purchase_vnd else 0

    # Tổng lợi nhuận BĐS (giá trị + tiền thuê)
    total_rent_earned = req.actual_rent * 12 * years_held * 1e6 if req.actual_rent else 0
    total_bds_profit = profit_vnd + total_rent_earned
    total_bds_pct = (total_bds_profit / purchase_vnd * 100) if purchase_vnd else 0

    return {
        "source": source,
        "estimated_min": round(estimated_min),
        "estimated_max": round(estimated_max),
        "estimated_avg": round(est_avg),
        "avg_m2": round(avg_m2),
        "sale_count": sale_count,
        "profit_vnd": round(profit_vnd),
        "profit_pct": round(profit_pct, 1),
        "years_held": round(years_held, 1),
        "cagr": round(cagr, 1),
        "actual_yield": round(actual_yield, 1),
        "rental_yield_market": round(rental_yield_market, 1),
        "total_rent_earned": round(total_rent_earned),
        "total_bds_profit": round(total_bds_profit),
        "total_bds_pct": round(total_bds_pct, 1),
        "savings_profit": round(savings_profit),
        "savings_profit_pct": round(savings_profit_pct, 1),
        "savings_rate": savings_rate,
        "district": district,
        "city": city
    }

# ============================================================
# ENDPOINT 2: MARKET — Listing count, xu hướng giá theo tháng
# ============================================================
class MarketRequest(BaseModel):
    district: str = ""
    city: str = ""
    bedrooms: str = ""
    project_name: str = ""

@router.post("/market")
async def report_market(req: MarketRequest, request: Request):
    check_rate(request.client.host)
    db = request.app.state.db

    district = resolve_district(req.district)
    city = resolve_city(req.city)
    slug = make_slug(req.project_name)
    bedrooms_int = None
    try:
        bedrooms_int = int(req.bedrooms)
    except (ValueError, TypeError):
        pass

    # --- Xu hướng giá theo tháng (cấp quận, căn hộ) ---
    trend_rows = await db.fetch(
        """SELECT month, avg_price_per_m2, median_price_per_m2, listing_count, bedrooms
           FROM price_history_monthly
           WHERE district = $1 AND city = $2
             AND property_type = 'Căn hộ'
           ORDER BY month ASC""",
        district, city
    )

    trend = []
    for r in trend_rows:
        trend.append({
            "month": r["month"].strftime("%Y-%m") if r["month"] else None,
            "avg_m2": round(float(r["avg_price_per_m2"])) if r["avg_price_per_m2"] else None,
            "median_m2": round(float(r["median_price_per_m2"])) if r["median_price_per_m2"] else None,
            "listing_count": int(r["listing_count"]) if r["listing_count"] else 0,
            "bedrooms": int(r["bedrooms"]) if r["bedrooms"] else None
        })

    # --- Tổng tin rao hiện tại (tháng mới nhất) ---
    latest = await db.fetchrow(
        """SELECT listing_count, avg_price_per_m2, median_price_per_m2, month
           FROM price_history_monthly
           WHERE district = $1 AND city = $2
             AND property_type = 'Căn hộ'
             AND bedrooms IS NULL
           ORDER BY month DESC LIMIT 1""",
        district, city
    )

    # --- MVP project listings nếu có ---
    project_listings = []
    if slug:
        mvp_rows = await db.fetch(
            """SELECT bedrooms, price_vnd, area_m2, price_per_m2, title
               FROM mvp_listings
               WHERE project_slug = $1
               ORDER BY price_vnd ASC LIMIT 20""",
            slug
        )
        for r in mvp_rows:
            project_listings.append({
                "bedrooms": int(r["bedrooms"]) if r["bedrooms"] else None,
                "price_vnd": float(r["price_vnd"]) if r["price_vnd"] else None,
                "area_m2": float(r["area_m2"]) if r["area_m2"] else None,
                "price_per_m2": float(r["price_per_m2"]) if r["price_per_m2"] else None,
                "title": r["title"]
            })

    # --- Rental data cho quận ---
    rental_row = await db.fetchrow(
        """SELECT COUNT(*) as cnt,
                  AVG(price_vnd) as avg_rent,
                  AVG(area_m2) as avg_area
           FROM rental_listings
           WHERE district = $1""",
        district
    )

    return {
        "district": district,
        "city": city,
        "trend": trend,
        "latest_month": latest["month"].strftime("%Y-%m") if latest and latest["month"] else None,
        "latest_listing_count": int(latest["listing_count"]) if latest and latest["listing_count"] else 0,
        "latest_avg_m2": round(float(latest["avg_price_per_m2"])) if latest and latest["avg_price_per_m2"] else 0,
        "latest_median_m2": round(float(latest["median_price_per_m2"])) if latest and latest["median_price_per_m2"] else 0,
        "project_listings": project_listings,
        "rental_count": int(rental_row["cnt"]) if rental_row else 0,
        "rental_avg_price": round(float(rental_row["avg_rent"])) if rental_row and rental_row["avg_rent"] else 0,
    }

# ============================================================
# ENDPOINT 3: CONTEXT — Infrastructure + News
# ============================================================
class ContextRequest(BaseModel):
    district: str = ""
    city: str = ""
    project_name: str = ""
    lat: Optional[float] = None
    lon: Optional[float] = None

@router.post("/context")
async def report_context(req: ContextRequest, request: Request):
    check_rate(request.client.host)
    db = request.app.state.db

    district = resolve_district(req.district)
    city = resolve_city(req.city)
    slug = make_slug(req.project_name)

    # --- Lấy lat/lon dự án nếu là MVP ---
    lat = req.lat
    lon = req.lon
    if not lat or not lon:
        if slug:
            proj = await db.fetchrow(
                "SELECT lat, lon FROM mvp_projects WHERE project_slug = $1", slug
            )
            if proj and proj["lat"]:
                lat = float(proj["lat"])
                lon = float(proj["lon"])

    # --- Infrastructure theo quận ---
    infra_rows = await db.fetch(
        """SELECT name, category, lat, lon, address, district
           FROM infrastructure
           WHERE district = $1 AND city = $2
           ORDER BY category
           LIMIT 200""",
        district, city
    )

    infrastructure = []
    for r in infra_rows:
        item = {
            "name": r["name"],
            "category": r["category"],
            "lat": float(r["lat"]) if r["lat"] else None,
            "lon": float(r["lon"]) if r["lon"] else None,
            "address": r["address"] if r["address"] else "",
        }
        # Tính khoảng cách nếu có tọa độ dự án
        if lat and lon and item["lat"] and item["lon"]:
            dlat = math.radians(item["lat"] - lat)
            dlon = math.radians(item["lon"] - lon)
            a = math.sin(dlat/2)**2 + math.cos(math.radians(lat)) * math.cos(math.radians(item["lat"])) * math.sin(dlon/2)**2
            item["distance_km"] = round(6371 * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a)), 2)
        infrastructure.append(item)

    # Nếu có MVP project_infrastructure, dùng data đã tính sẵn
    project_infra = []
    if slug:
        pi_rows = await db.fetch(
            """SELECT i.name, i.category, i.lat, i.lon, pi.distance_km
               FROM project_infrastructure pi
               JOIN infrastructure i ON i.id = pi.infrastructure_id
               WHERE pi.project_slug = $1
               ORDER BY pi.distance_km ASC
               LIMIT 50""",
            slug
        )
        for r in pi_rows:
            project_infra.append({
                "name": r["name"],
                "category": r["category"],
                "lat": float(r["lat"]) if r["lat"] else None,
                "lon": float(r["lon"]) if r["lon"] else None,
                "distance_km": float(r["distance_km"]) if r["distance_km"] else None,
            })

    # --- News theo quận hoặc thành phố ---
    news_rows = await db.fetch(
        """SELECT title, url, pub_date, source
           FROM news_articles
           WHERE (title ILIKE $1 OR title ILIKE $2)
           ORDER BY pub_date DESC
           LIMIT 10""",
        f"%{district}%", f"%{req.project_name}%"
    )

    news = []
    for r in news_rows:
        news.append({
            "title": r["title"],
            "url": r["url"],
            "date": r["pub_date"].strftime("%Y-%m-%d") if r["pub_date"] else None,
            "source": r["source"] if r["source"] else "",
        })

    return {
        "district": district,
        "city": city,
        "project_lat": lat,
        "project_lon": lon,
        "infrastructure": infrastructure,
        "project_infrastructure": project_infra,
        "news": news,
    }
