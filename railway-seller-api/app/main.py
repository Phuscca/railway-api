from fastapi import FastAPI
from app.db.database import connect_db, close_db
from app.routers import seller, admin

app = FastAPI(title="BDSTT Seller API", version="0.2.0")


@app.on_event('startup')
async def on_startup():
    await connect_db()


@app.on_event('shutdown')
async def on_shutdown():
    await close_db()


app.include_router(seller.router)
app.include_router(admin.router)


@app.get('/health')
async def health():
    return {'status': 'ok'}
