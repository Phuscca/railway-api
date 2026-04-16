"""
Endpoint công khai: ước tính giá căn hộ.
Ưu tiên data MVP → fallback district → trả not_found.
KHÔNG cần API key.
"""

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from app.db.database import get_pool
import time
from collections import defaultdict

router = APIRouter(prefix='/v1/estimate', tags=['estimate'])

# Rate limit giống conversation
_rate_store: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT = 30
RATE_WINDOW = 60

def check_rate_limit(ip: str):
    now = time.time()
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT:
        raise HTTPException(status_code=429, detail='Rate limit exceeded')
    _rate_store[ip].append(now)


# === Mapping tên quận ===
DISTRICT_ALIASES = {
    # HCM - số
    'quận 1': '1', 'quan 1': '1', 'q1': '1', 'q.1': '1',
    'quận 2': 'Thủ Đức', 'quan 2': 'Thủ Đức', 'q2': 'Thủ Đức', 'q.2': 'Thủ Đức',
    'quận 3': '3', 'quan 3': '3', 'q3': '3', 'q.3': '3',
    'quận 4': '4', 'quan 4': '4', 'q4': '4', 'q.4': '4',
    'quận 5': '5', 'quan 5': '5', 'q5': '5', 'q.5': '5',
    'quận 6': '6', 'quan 6': '6', 'q6': '6', 'q.6': '6',
    'quận 7': '7', 'quan 7': '7', 'q7': '7', 'q.7': '7',
    'quận 8': '8', 'quan 8': '8', 'q8': '8', 'q.8': '8',
    'quận 9': 'Thủ Đức', 'quan 9': 'Thủ Đức', 'q9': 'Thủ Đức', 'q.9': 'Thủ Đức',
    'quận 10': '10', 'quan 10': '10', 'q10': '10', 'q.10': '10',
    'quận 11': '11', 'quan 11': '11', 'q11': '11', 'q.11': '11',
    'quận 12': '12', 'quan 12': '12', 'q12': '12', 'q.12': '12',
    # HCM - tên
    'bình thạnh': 'Bình Thạnh', 'binh thanh': 'Bình Thạnh',
    'gò vấp': 'Gò Vấp', 'go vap': 'Gò Vấp',
    'tân bình': 'Tân Bình', 'tan binh': 'Tân Bình',
    'tân phú': 'Tân Phú', 'tan phu': 'Tân Phú',
    'phú nhuận': 'Phú Nhuận', 'phu nhuan': 'Phú Nhuận',
    'bình tân': 'Bình Tân', 'binh tan': 'Bình Tân',
    'thủ đức': 'Thủ Đức', 'thu duc': 'Thủ Đức',
    'tp thủ đức': 'Thủ Đức', 'tp thu duc': 'Thủ Đức',
    'nhà bè': 'Nhà Bè', 'nha be': 'Nhà Bè',
    'bình chánh': 'Bình Chánh', 'binh chanh': 'Bình Chánh',
    'hóc môn': 'Hóc Môn', 'hoc mon': 'Hóc Môn',
    'cần giờ': 'Cần Giờ', 'can gio': 'Cần Giờ',
    # HN
    'ba đình': 'Ba Đình', 'ba dinh': 'Ba Đình',
    'hoàn kiếm': 'Hoàn Kiếm', 'hoan kiem': 'Hoàn Kiếm',
    'hai bà trưng': 'Hai Bà Trưng', 'hai ba trung': 'Hai Bà Trưng',
    'đống đa': 'Đống Đa', 'dong da': 'Đống Đa',
    'cầu giấy': 'Cầu Giấy', 'cau giay': 'Cầu Giấy',
    'thanh xuân': 'Thanh Xuân', 'thanh xuan': 'Thanh Xuân',
    'hoàng mai': 'Hoàng Mai', 'hoang mai': 'Hoàng Mai',
    'long biên': 'Long Biên', 'long bien': 'Long Biên',
    'nam từ liêm': 'Nam Từ Liêm', 'nam tu liem': 'Nam Từ Liêm',
    'bắc từ liêm': 'Bắc Từ Liêm', 'bac tu liem': 'Bắc Từ Liêm',
    'hà đông': 'Hà Đông', 'ha dong': 'Hà Đông',
    'tây hồ': 'Tây Hồ', 'tay ho': 'Tây Hồ',
    'thanh trì': 'Thanh Trì', 'thanh tri': 'Thanh Trì',
    'gia lâm': 'Gia Lâm', 'gia lam': 'Gia Lâm',
    'đông anh': 'Đông Anh', 'dong anh': 'Đông Anh',
    'hoài đức': 'Hoài Đức', 'hoai duc': 'Hoài Đức',
    # Bình Dương
    'dĩ an': 'Dĩ An', 'di an': 'Dĩ An',
    'thuận an': 'Thuận An', 'thuan an': 'Thuận An',
    'thủ dầu một': 'Thủ Dầu Một', 'thu dau mot': 'Thủ Dầu Một',
}

CITY_ALIASES = {
    'hồ chí minh': 'Hồ Chí Minh', 'ho chi minh': 'Hồ Chí Minh',
    'hcm': 'Hồ Chí Minh', 'tp hcm': 'Hồ Chí Minh', 'tphcm': 'Hồ Chí Minh',
    'sài gòn': 'Hồ Chí Minh', 'sai gon': 'Hồ Chí Minh',
    'hà nội': 'Hà Nội', 'ha noi': 'Hà Nội', 'hn': 'Hà Nội',
    'bình dương': 'Bình Dương', 'binh duong': 'Bình Dương',
    'đồng nai': 'Đồng Nai', 'dong nai': 'Đồng Nai',
    'long an': 'Long An',
    'đà nẵng': 'Đà Nẵng', 'da nang': 'Đà Nẵng',
}

def normalize_district(raw: str) -> str:
    key = raw.strip().lower()
    return DISTRICT_ALIASES.get(key, raw.strip())

def normalize_city(raw: str) -> str:
    key = raw.strip().lower()
    return CITY_ALIASES.get(key, raw.strip())

def make_project_slug(name: str) -> str:
    import re, unicodedata
    s = unicodedata.normalize('NFD', name.lower())
    s = ''.join(c for c in s if unicodedata.category(c) != 'Mn')
    s = re.sub(r'[^a-z0-9]+', '-', s).strip('-')
    return s


class EstimateRequest(BaseModel):
    project_name: str = Field(min_length=1, max_length=200)
    city: str = Field(min_length=1, max_length=100)
    district: str = Field(min_length=1, max_length=100)
    bedrooms: str = Field(min_length=1, max_length=10)
    area_sqm: float = Field(gt=0, le=500)
    purchase_price: float = Field(gt=0, le=100)  # tỷ VNĐ
    purchase_year: int = Field(ge=2005, le=2030)
    purchase_month: int = Field(ge=1, le=12)


@router.post('/price')
async def estimate_price(payload: EstimateRequest, request: Request):
    client_ip = request.headers.get('x-forwarded-for', request.client.host)
    if client_ip and ',' in client_ip:
        client_ip = client_ip.split(',')[0].strip()
    check_rate_limit(client_ip)

    district = normalize_district(payload.district)
    city = normalize_city(payload.city)
    slug = make_project_slug(payload.project_name)

    # Parse bedrooms thành int
    bed_int = None
    try:
        bed_int = int(payload.bedrooms)
    except ValueError:
        if payload.bedrooms.lower() == 'studio':
            bed_int = 0
        elif payload.bedrooms == '4+':
            bed_int = 4

    pool = get_pool()
    result = {
        'source': None,
        'estimated_min': None,
        'estimated_max': None,
        'estimated_avg_m2': None,
        'rental_yield': None,
        'sale_count': None,
        'listing_count': None,
        'district': district,
        'city': city,
        'slug': slug,
    }

    async with pool.acquire() as conn:
        # === TRY 1: MVP valuation ===
        mvp = await conn.fetchrow(
            '''SELECT estimated_low, estimated_high, sale_median_m2,
                      rental_yield, sale_count
               FROM mvp_valuation
               WHERE project_slug = $1 AND bedrooms = $2''',
            slug, bed_int
        )

        if mvp and mvp['estimated_low']:
            result['source'] = 'project'
            result['estimated_min'] = int(mvp['estimated_low'])
            result['estimated_max'] = int(mvp['estimated_high'])
            result['estimated_avg_m2'] = int(mvp['sale_median_m2'])
            result['rental_yield'] = float(mvp['rental_yield']) if mvp['rental_yield'] else None
            result['sale_count'] = int(mvp['sale_count'])

        else:
            # === TRY 2: District average ===
            dist = await conn.fetchrow(
                '''SELECT avg_price_per_m2, median_price_per_m2, listing_count
                   FROM price_history_monthly
                   WHERE district = $1 AND city = $2
                     AND property_type = 'Căn hộ'
                   ORDER BY month DESC
                   LIMIT 1''',
                district, city
            )

            if dist and dist['median_price_per_m2']:
                m2_price = int(dist['median_price_per_m2'])
                est_total = m2_price * payload.area_sqm
                # ±10% range vì là trung bình quận
                result['source'] = 'district'
                result['estimated_min'] = int(est_total * 0.9)
                result['estimated_max'] = int(est_total * 1.1)
                result['estimated_avg_m2'] = m2_price
                result['listing_count'] = int(dist['listing_count'])

            elif dist and dist['avg_price_per_m2']:
                m2_price = int(dist['avg_price_per_m2'])
                est_total = m2_price * payload.area_sqm
                result['source'] = 'district_avg'
                result['estimated_min'] = int(est_total * 0.85)
                result['estimated_max'] = int(est_total * 1.15)
                result['estimated_avg_m2'] = m2_price
                result['listing_count'] = int(dist['listing_count'])

            else:
                result['source'] = 'not_found'

    return result
