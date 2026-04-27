# app/server.py
import asyncio
from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from dotenv import load_dotenv
from app.init_db import init_database
# Import routers
from app.api.metaconnect_routes import router as metaconnect_router
from app.api.auth_routes import router as auth_router
from app.api.admin_routes import router as admin_router
from app.api.dashboard_routes import router as dashboard_router
from app.api.cycle_calculator_routes import router as cycle_calculator_router
from app.api.vps_routes import router as vps_router 
from hedgebridge.listener_manager import listener_manager

load_dotenv()

app = FastAPI(title="Hedge Bridge", version="2.0")

# Serve static files
app.mount("/static", StaticFiles(directory="static"), name="static")


# ========================
# STARTUP EVENT
# ========================
@app.on_event("startup")
async def startup_event():
    print("🚀 Starting up - Creating database tables if they don't exist...")
    await init_database()
    if not hasattr(app.state, "listener_task"):
        app.state.listener_task = asyncio.create_task(listener_manager.start())


# =========================
# SERVE PAGES (Static HTML)
# =========================
@app.get("/")
async def serve_login():
    return FileResponse("static/login.html")

@app.get("/signup")
async def serve_signup():
    return FileResponse("static/signup.html")


@app.get("/profile-manage")
async def serve_profile_manage():
    return FileResponse("static/profile-manage.html")


@app.get("/profile-activity")
async def serve_profile_activity():
    return FileResponse("static/profile-activity.html")


@app.get("/app")
async def serve_dashboard():
    return FileResponse("static/dashboard.html")


@app.get("/mt5-connect")
async def serve_mt5_connect():
    return FileResponse("static/mt5-connect.html")

@app.get("/vps-connect")                          # ← NEW
async def serve_vps_connect():
    return FileResponse("static/vps-connect.html")

@app.get("/cycle-calculator")
async def serve_cycle_calculator():
    return FileResponse("static/cycle-calculator.html")


# =========================
# Include API Routers
# =========================
app.include_router(auth_router)
app.include_router(admin_router)
app.include_router(metaconnect_router)
app.include_router(dashboard_router)        # ← Added this
app.include_router(cycle_calculator_router)
app.include_router(vps_router)                    # ← NEW



@app.get("/health")
async def health_check():
    return {"status": "ok", "message": "Hedge Bridge API is running"}



