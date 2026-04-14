from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from app.db.database import connect_db, close_db
from app.routers import seller, admin
import traceback

app = FastAPI(title="BDSTT Seller API", version="0.2.0")


@app.on_event('startup')
async def on_startup():
    await connect_db()


@app.on_event('shutdown')
async def on_shutdown():
    await close_db()


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    return JSONResponse(
        status_code=500,
        content={
            "error_type": type(exc).__name__,
            "error_message": str(exc),
            "traceback": traceback.format_exc()
        }
    )


app.include_router(seller.router)
app.include_router(admin.router)


@app.get('/health')
async def health():
    return {'status': 'ok'}
