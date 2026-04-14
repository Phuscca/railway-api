import json
from uuid import uuid4
from app.db.database import get_pool


async def insert_lead_event(session_id: str | None, property_id: str | None, event_type: str, event_value: str | None = None, meta: dict | None = None):
    pool = get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            '''
            insert into bdstt_lead_events (session_id, property_id, event_type, event_value, event_meta_json)
            values ($1, $2::uuid, $3, $4, $5::jsonb)
            ''',
            session_id,
            property_id,
            event_type,
            event_value,
            json.dumps(meta or {}),
        )


async def create_property_and_calculation(payload, summary: dict, scenarios: list[dict], session_id: str):
    pool = get_pool()
    property_id = str(uuid4())
    calculation_id = str(uuid4())

    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(
                '''
                insert into bdstt_properties (
                  id, property_type, district, project_name, area_net, bedrooms
                ) values ($1::uuid, 'apartment', $2, $3, $4, $5)
                ''',
                property_id,
                payload.district,
                payload.project_name,
                payload.area_net,
                payload.bedrooms,
            )

            await conn.execute(
                '''
                insert into bdstt_sale_calculations (
                  id, property_id, session_id, input_sale_price, input_brokerage_mode,
                  input_brokerage_value, input_loan_outstanding, target_net_proceeds,
                  estimated_pit_tax, estimated_brokerage_fee, estimated_notary_fee,
                  estimated_other_costs, estimated_net_proceeds, result_json
                ) values (
                  $1::uuid, $2::uuid, $3, $4, $5,
                  $6, $7, $8,
                  $9, $10, $11,
                  $12, $13, $14::jsonb
                )
                ''',
                calculation_id,
                property_id,
                session_id,
                payload.expected_sale_price,
                payload.brokerage_mode,
                payload.brokerage_value,
                payload.outstanding_loan,
                payload.target_net_proceeds,
                summary['pit_tax'],
                summary['brokerage_fee'],
                summary['notary_fee'],
                summary['other_costs'],
                summary['estimated_net_proceeds'],
                json.dumps({
                    'summary': summary,
                    'scenarios': scenarios,
                    'input': payload.model_dump(),
                }),
            )

    await insert_lead_event(session_id, property_id, 'calculated', None, {'project_name': payload.project_name})
    return {'property_id': property_id, 'calculation_id': calculation_id}


async def create_telegram_link(session_id: str):
    pool = get_pool()
    token = f'tg_{uuid4().hex[:10]}'
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'select property_id from bdstt_sale_calculations where session_id = $1 order by created_at desc limit 1',
            session_id,
        )
        property_id = str(row['property_id']) if row and row['property_id'] else None
        await conn.execute(
            '''
            insert into bdstt_telegram_links (session_id, link_token, status)
            values ($1, $2, 'pending')
            ''',
            session_id,
            token,
        )
    await insert_lead_event(session_id, property_id, 'telegram_link_created', token, None)
    return token


async def confirm_telegram_link(link_token: str, telegram_chat_id: str, telegram_username: str | None = None):
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            link = await conn.fetchrow(
                '''
                select session_id from bdstt_telegram_links
                where link_token = $1 and status = 'pending'
                limit 1
                ''',
                link_token,
            )
            if not link:
                return None

            await conn.execute(
                '''
                update bdstt_telegram_links
                set telegram_chat_id = $1, linked_at = now(), status = 'linked'
                where link_token = $2
                ''',
                telegram_chat_id,
                link_token,
            )

            calc = await conn.fetchrow(
                'select property_id from bdstt_sale_calculations where session_id = $1 order by created_at desc limit 1',
                link['session_id'],
            )
            property_id = str(calc['property_id']) if calc and calc['property_id'] else None

    await insert_lead_event(link['session_id'], property_id, 'telegram_connected', telegram_username or telegram_chat_id, None)
    return {'session_id': link['session_id'], 'property_id': property_id}


async def admin_overview():
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            '''
            select event_type, count(*) as total
            from bdstt_lead_events
            group by event_type
            '''
        )
    counts = {row['event_type']: row['total'] for row in rows}
    return {
        'page_views': counts.get('page_view', 0),
        'started': counts.get('started', 0),
        'calculated': counts.get('calculated', 0),
        'detail_unlocked': counts.get('detail_unlocked', 0),
        'telegram_connected': counts.get('telegram_connected', 0),
        'valuation_ready': counts.get('valuation_ready', 0),
    }


async def admin_funnel():
    counts = await admin_overview()
    return {
        'steps': [
            {'label': 'Page view', 'count': counts['page_views']},
            {'label': 'Started', 'count': counts['started']},
            {'label': 'Calculated', 'count': counts['calculated']},
            {'label': 'Unlock detail', 'count': counts['detail_unlocked']},
            {'label': 'Telegram connected', 'count': counts['telegram_connected']},
        ]
    }
