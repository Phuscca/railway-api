from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from app.db.database import connect_db, close_db, get_pool
from app.routers import seller, admin, conversation
import traceback

app = FastAPI(title="BDSTT API", version="0.3.0")

# ============================================================
# CORS — cho phép website WordPress gọi API
# ============================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://bdstrongtamtay.com",
        "https://www.bdstrongtamtay.com",
        "http://localhost",           # để test local
        "http://localhost:8080",
    ],
    allow_credentials=False,
    allow_methods=["POST", "GET"],
    allow_headers=["Content-Type", "x-api-key"],
)


@app.on_event("startup")
async def on_startup():
    await connect_db()


@app.on_event("shutdown")
async def on_shutdown():
    await close_db()


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc(),
        },
    )


# ============================================================
# ROUTERS
# ============================================================
app.include_router(seller.router)         # /v1/seller/*  — cần API key
app.include_router(admin.router)          # /v1/admin/*   — cần API key
app.include_router(conversation.router)   # /v1/conversation/* — CÔNG KHAI


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "build": "0.3.0-conversation-ui"
    }


@app.get("/debug/db")
async def debug_db():
    pool = get_pool()
    async with pool.acquire() as conn:
        now_value = await conn.fetchval("select now()")
        lead_events_count = await conn.fetchval(
            "select count(*) from bdstt_lead_events"
        )
        mua_nha_count = await conn.fetchval(
            "select count(*) from conversation_mua_nha"
        )
        chu_nha_count = await conn.fetchval(
            "select count(*) from conversation_chu_nha"
        )
        return {
            "status": "ok",
            "build": "0.3.0-conversation-ui",
            "db_now": str(now_value),
            "lead_events_count": int(lead_events_count),
            "conversation_mua_nha": int(mua_nha_count),
            "conversation_chu_nha": int(chu_nha_count),
        }
