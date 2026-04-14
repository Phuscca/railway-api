from uuid import uuid4
from fastapi import APIRouter, Header, HTTPException
from app.db.database import get_settings
from app.db.repository import create_property_and_calculation, create_telegram_link, confirm_telegram_link, insert_lead_event
from app.schemas.seller import CalculateRequest, TelegramLinkRequest
from app.schemas.tracking import EventTrackRequest, TelegramConfirmRequest
from app.services.calculator import calculate_net_proceeds, build_scenarios

router = APIRouter(prefix='/v1/seller', tags=['seller'])


def guard(x_api_key: str | None):
    if x_api_key != get_settings()['api_key']:
        raise HTTPException(status_code=401, detail='Unauthorized')


@router.post('/track')
async def track(payload: EventTrackRequest, x_api_key: str | None = Header(default=None)):
    guard(x_api_key)
    await insert_lead_event(payload.session_id, None, payload.event_type, payload.event_value, payload.meta)
    return {'success': True}


@router.post('/calculate')
async def calculate(payload: CalculateRequest, x_api_key: str | None = Header(default=None)):
    guard(x_api_key)
    summary = calculate_net_proceeds(payload)
    scenarios = build_scenarios(payload)
    session_id = f'sess_{uuid4().hex[:12]}'
    ids = await create_property_and_calculation(payload, summary, scenarios, session_id)

    return {
        'success': True,
        'session_id': session_id,
        'calculation_id': ids['calculation_id'],
        'property_id': ids['property_id'],
        'summary': summary,
        'scenarios': scenarios,
    }


@router.post('/create-telegram-link')
async def create_tg_link(payload: TelegramLinkRequest, x_api_key: str | None = Header(default=None)):
    guard(x_api_key)
    if not payload.session_id:
        raise HTTPException(status_code=400, detail='session_id is required')

    token = await create_telegram_link(payload.session_id)
    bot_username = get_settings()['bot_username']
    return {
        'success': True,
        'link_token': token,
        'telegram_deep_link': f'https://t.me/{bot_username}?start=seller_{token}',
    }


@router.post('/telegram-link/confirm')
async def confirm_tg_link(payload: TelegramConfirmRequest, x_api_key: str | None = Header(default=None)):
    guard(x_api_key)
    result = await confirm_telegram_link(payload.link_token, payload.telegram_chat_id, payload.telegram_username)
    if not result:
        raise HTTPException(status_code=404, detail='Invalid or expired link token')
    return {'success': True, **result}
