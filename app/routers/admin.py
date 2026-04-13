from fastapi import APIRouter, Header, HTTPException
from app.db.database import get_settings
from app.db.repository import admin_overview, admin_funnel

router = APIRouter(prefix='/v1/admin', tags=['admin'])


def guard(x_api_key: str | None):
    if x_api_key != get_settings()['api_key']:
        raise HTTPException(status_code=401, detail='Unauthorized')


@router.get('/overview')
async def overview(x_api_key: str | None = Header(default=None)):
    guard(x_api_key)
    return await admin_overview()


@router.get('/funnel')
async def funnel(x_api_key: str | None = Header(default=None)):
    guard(x_api_key)
    return await admin_funnel()
