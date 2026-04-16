"""
Router công khai cho Conversation UI.
- KHÔNG yêu cầu API key
- CHỈ cho phép INSERT/UPDATE data conversation
- Có rate limit
- KHÔNG cho phép đọc data ra ngoài
"""

import time
import json
from collections import defaultdict
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel, Field
from app.db.database import get_pool

router = APIRouter(prefix='/v1/conversation', tags=['conversation'])

# ============================================================
# RATE LIMITER — đơn giản, chạy trong memory
# Giới hạn: 30 request / phút / IP
# ============================================================
_rate_store: dict[str, list[float]] = defaultdict(list)
RATE_LIMIT = 30        # số request tối đa
RATE_WINDOW = 60       # trong 60 giây

def check_rate_limit(ip: str):
    now = time.time()
    # Xóa các timestamp cũ hơn window
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < RATE_WINDOW]
    if len(_rate_store[ip]) >= RATE_LIMIT:
        raise HTTPException(
            status_code=429,
            detail='Quá nhiều request. Vui lòng thử lại sau.'
        )
    _rate_store[ip].append(now)


# ============================================================
# SCHEMA — Chỉ chấp nhận đúng cấu trúc này
# ============================================================
# Danh sách field hợp lệ cho từng flow — chặn mọi thứ khác
ALLOWED_FIELDS = {
    'mua_nha': {
        'flow', 'purpose', 'income_range', 'savings_range',
        'loan_willingness', 'budget_min', 'budget_max',
        'family_size', 'has_young_children', 'work_location',
        'priorities', 'property_type_preference',
        'recommended_areas', 'area_clicked',
    },
    'chu_nha': {
        'flow', 'project_name', 'city', 'district', 'bedrooms', 'area_sqm', 'building',
        'purchase_price', 'purchase_month', 'purchase_year',
        'current_status', 'actual_rent', 'furnishing', 'estimated_rent',
        'sell_intention', 'expected_sell_price', 'sell_reason',
    },
}

# Field nào cần lưu dạng số
NUMERIC_FIELDS = {
    'budget_min', 'budget_max', 'area_sqm',
    'purchase_price', 'purchase_month', 'purchase_year',
    'actual_rent', 'estimated_rent', 'expected_sell_price',
}

# Field nào cần lưu dạng JSON
JSON_FIELDS = {'priorities', 'recommended_areas'}


class SaveFieldRequest(BaseModel):
    session_id: str = Field(min_length=5, max_length=100)
    flow: str = Field(min_length=1, max_length=20)
    field: str = Field(min_length=1, max_length=50)
    value: str = Field(max_length=500)


# ============================================================
# ENDPOINT CÔNG KHAI — CHỈ CHO PHÉP GHI
# ============================================================
@router.post('/save-field')
async def save_field(payload: SaveFieldRequest, request: Request):
    # 1. Rate limit theo IP
    client_ip = request.headers.get('x-forwarded-for', request.client.host)
    if client_ip and ',' in client_ip:
        client_ip = client_ip.split(',')[0].strip()
    check_rate_limit(client_ip)

    # 2. Validate flow
    if payload.flow not in ('mua_nha', 'chu_nha'):
        raise HTTPException(status_code=400, detail='Invalid flow')

    # 3. Validate field name — chặn SQL injection qua tên cột
    if payload.field not in ALLOWED_FIELDS[payload.flow]:
        raise HTTPException(status_code=400, detail='Invalid field')

    # 4. Xác định bảng
    table = 'conversation_mua_nha' if payload.flow == 'mua_nha' else 'conversation_chu_nha'

    # 5. Chuyển đổi kiểu dữ liệu
    if payload.field in NUMERIC_FIELDS:
        try:
            db_value = float(payload.value)
        except ValueError:
            raise HTTPException(status_code=400, detail='Invalid numeric value')
    elif payload.field in JSON_FIELDS:
        try:
            db_value = payload.value  # đã là JSON string từ frontend
            json.loads(db_value)      # validate JSON hợp lệ
        except json.JSONDecodeError:
            raise HTTPException(status_code=400, detail='Invalid JSON value')
    else:
        db_value = payload.value

    # 6. UPSERT — tạo session mới hoặc cập nhật field
    pool = get_pool()
    user_agent = request.headers.get('user-agent', '')[:500]

    async with pool.acquire() as conn:
        if payload.field in NUMERIC_FIELDS:
            await conn.execute(
                f'''
                INSERT INTO {table} (session_id, {payload.field}, last_step, ip_address, user_agent)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (session_id)
                DO UPDATE SET
                    {payload.field} = $2,
                    last_step = $3,
                    updated_at = now()
                ''',
                payload.session_id,
                db_value,
                payload.field,
                client_ip,
                user_agent,
            )
        elif payload.field in JSON_FIELDS:
            await conn.execute(
                f'''
                INSERT INTO {table} (session_id, {payload.field}, last_step, ip_address, user_agent)
                VALUES ($1, $2::jsonb, $3, $4, $5)
                ON CONFLICT (session_id)
                DO UPDATE SET
                    {payload.field} = $2::jsonb,
                    last_step = $3,
                    updated_at = now()
                ''',
                payload.session_id,
                db_value,
                payload.field,
                client_ip,
                user_agent,
            )
        else:
            await conn.execute(
                f'''
                INSERT INTO {table} (session_id, {payload.field}, last_step, ip_address, user_agent)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (session_id)
                DO UPDATE SET
                    {payload.field} = $2,
                    last_step = $3,
                    updated_at = now()
                ''',
                payload.session_id,
                db_value,
                payload.field,
                client_ip,
                user_agent,
            )

    return {'ok': True}


# ============================================================
# ENDPOINT ĐÁNH DẤU HOÀN THÀNH — cũng công khai
# ============================================================
class CompleteRequest(BaseModel):
    session_id: str = Field(min_length=5, max_length=100)
    flow: str = Field(min_length=1, max_length=20)


@router.post('/complete')
async def mark_complete(payload: CompleteRequest, request: Request):
    client_ip = request.headers.get('x-forwarded-for', request.client.host)
    if client_ip and ',' in client_ip:
        client_ip = client_ip.split(',')[0].strip()
    check_rate_limit(client_ip)

    if payload.flow not in ('mua_nha', 'chu_nha'):
        raise HTTPException(status_code=400, detail='Invalid flow')

    table = 'conversation_mua_nha' if payload.flow == 'mua_nha' else 'conversation_chu_nha'

    pool = get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            f'''
            UPDATE {table}
            SET completed = true, updated_at = now()
            WHERE session_id = $1
            ''',
            payload.session_id,
        )

    return {'ok': True}
