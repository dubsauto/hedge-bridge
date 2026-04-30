# app/services/guacamole.py
# app/services/guacamole.py
import os
import base64
import json
import httpx
import time
from typing import Optional

GUAC_BASE_URL = os.getenv("GUAC_BASE_URL", "http://localhost:8080")
GUAC_ADMIN_USER = os.getenv("GUAC_ADMIN_USER", "guacadmin")
GUAC_ADMIN_PASS = os.getenv("GUAC_ADMIN_PASS", "guacadmin")
GUAC_PUBLIC_URL = os.getenv("GUAC_PUBLIC_URL", GUAC_BASE_URL)

DATA_SOURCE = "mysql"

# ─────────────────────────────────────────────────────────────
# TOKEN CACHE — reuse the same token across all tabs
# Guacamole sessions last 60 min; we refresh after 50 min
# ─────────────────────────────────────────────────────────────
_cached_token: Optional[str] = None
_token_expiry: float = 0


class GuacamoleError(Exception):
    pass


async def _get_token() -> str:
    global _cached_token, _token_expiry

    # Return cached token if still valid
    if _cached_token and time.time() < _token_expiry:
        return _cached_token

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{GUAC_BASE_URL}/api/tokens",
            data={"username": GUAC_ADMIN_USER, "password": GUAC_ADMIN_PASS},
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            timeout=10,
        )
    if resp.status_code != 200:
        raise GuacamoleError(f"Guacamole auth failed: {resp.status_code} {resp.text}")

    token = resp.json()["authToken"]
    _cached_token = token
    _token_expiry = time.time() + (50 * 60)  # cache for 50 minutes
    return token


async def _find_connection(token: str, name: str) -> Optional[str]:
    async with httpx.AsyncClient() as client:
        resp = await client.get(
            f"{GUAC_BASE_URL}/api/session/data/{DATA_SOURCE}/connections",
            params={"token": token},
            timeout=10,
        )
    if resp.status_code != 200:
        return None
    for conn_id, conn in resp.json().items():
        if conn.get("name") == name:
            return conn_id
    return None


async def _create_connection(
    token: str,
    name: str,
    protocol: str,
    hostname: str,
    port: int,
    username: str,
    password: str,
) -> str:
    params: dict = {
        "hostname": hostname,
        "port": str(port),
        "username": username,
        "password": password,
    }
    if protocol == "ssh":
        params.update({"color-scheme": "green-black", "font-size": "14"})
    elif protocol == "rdp":
        params.update({"ignore-cert": "true", "security": "any"})

    payload = {
        "name": name,
        "protocol": protocol,
        "parentIdentifier": "ROOT",
        "parameters": params,
        "attributes": {
            "max-connections": "10",
            "max-connections-per-user": "10",
        },
    }

    async with httpx.AsyncClient() as client:
        resp = await client.post(
            f"{GUAC_BASE_URL}/api/session/data/{DATA_SOURCE}/connections",
            params={"token": token},
            json=payload,
            timeout=10,
        )

    if resp.status_code not in (200, 201):
        raise GuacamoleError(f"Failed to create connection: {resp.status_code} {resp.text}")

    return resp.json()["identifier"]


def _build_client_url(token: str, connection_id: str) -> str:
    raw = f"{connection_id}\x00c\x00{DATA_SOURCE}"
    client_id = base64.b64encode(raw.encode()).decode()
    return f"{GUAC_PUBLIC_URL}/#/client/{client_id}?token={token}"


async def get_launch_url(
    vps_host: str,
    vps_username: str,
    vps_password: str,
    protocol: str = "ssh",
    port: Optional[int] = None,
) -> str:
    if port is None:
        port = 22 if protocol == "ssh" else 3389

    conn_name = f"HedgeBridge | {vps_host} ({vps_username})"

    token = await _get_token()

    conn_id = await _find_connection(token, conn_name)
    if not conn_id:
        conn_id = await _create_connection(
            token=token,
            name=conn_name,
            protocol=protocol,
            hostname=vps_host,
            port=port,
            username=vps_username,
            password=vps_password,
        )

    return _build_client_url(token, conn_id)



# # app/services/guacamole.py
# """
# Guacamole integration service for Hedge Bridge.

# Flow when a user clicks the 🖥️ terminal button:
#   1. Dashboard JS calls  GET /vps/accounts/{id}/launch
#   2. This service authenticates with Guacamole's REST API using the
#      admin credentials stored in .env
#   3. It creates (or reuses) a named connection for that VPS
#   4. It returns a one-time-use Guacamole client URL
#   5. Dashboard opens that URL in a new tab → full remote desktop in browser

# Supported protocols:
#   - SSH  (Linux VPS — most common for HedgeBridge VPS entries)
#   - RDP  (Windows VPS)
# """

# import os
# import base64
# import json
# import httpx
# from typing import Optional

# GUAC_BASE_URL = os.getenv("GUAC_BASE_URL", "http://localhost:8080")
# GUAC_ADMIN_USER = os.getenv("GUAC_ADMIN_USER", "guacadmin")
# GUAC_ADMIN_PASS = os.getenv("GUAC_ADMIN_PASS", "guacadmin")
# GUAC_PUBLIC_URL = os.getenv("GUAC_PUBLIC_URL", GUAC_BASE_URL)

# # Guacamole stores connections in this data source when using MySQL
# DATA_SOURCE = "mysql"


# class GuacamoleError(Exception):
#     pass


# # ─────────────────────────────────────────────────────────────
# # AUTH — get a session token from Guacamole's REST API
# # ─────────────────────────────────────────────────────────────
# async def _get_token() -> str:
#     async with httpx.AsyncClient() as client:
#         resp = await client.post(
#             f"{GUAC_BASE_URL}/api/tokens",
#             data={"username": GUAC_ADMIN_USER, "password": GUAC_ADMIN_PASS},
#             headers={"Content-Type": "application/x-www-form-urlencoded"},
#             timeout=10,
#         )
#     if resp.status_code != 200:
#         raise GuacamoleError(f"Guacamole auth failed: {resp.status_code} {resp.text}")
#     return resp.json()["authToken"]


# # ─────────────────────────────────────────────────────────────
# # FIND EXISTING CONNECTION by name
# # ─────────────────────────────────────────────────────────────
# async def _find_connection(token: str, name: str) -> Optional[str]:
#     """Return the connection identifier string if a connection with `name` exists."""
#     async with httpx.AsyncClient() as client:
#         resp = await client.get(
#             f"{GUAC_BASE_URL}/api/session/data/{DATA_SOURCE}/connections",
#             params={"token": token},
#             timeout=10,
#         )
#     if resp.status_code != 200:
#         return None
#     connections = resp.json()
#     for conn_id, conn in connections.items():
#         if conn.get("name") == name:
#             return conn_id
#     return None


# # ─────────────────────────────────────────────────────────────
# # CREATE CONNECTION
# # ─────────────────────────────────────────────────────────────
# async def _create_connection(
#     token: str,
#     name: str,
#     protocol: str,      # "ssh" | "rdp"
#     hostname: str,
#     port: int,
#     username: str,
#     password: str,
# ) -> str:
#     """Create a Guacamole connection and return its identifier string."""

#     params: dict = {
#         "hostname": hostname,
#         "port": str(port),
#         "username": username,
#         "password": password,
#     }

#     if protocol == "ssh":
#         params.update({
#             "color-scheme": "green-black",
#             "font-size": "14",
#         })
#     elif protocol == "rdp":
#         params.update({
#             "ignore-cert": "true",
#             "security": "any",
#         })

#     payload = {
#         "name": name,
#         "protocol": protocol,
#         "parentIdentifier": "ROOT",
#         "parameters": params,
#         "attributes": {
#             "max-connections": "5",
#             "max-connections-per-user": "5",
#         },
#     }

#     async with httpx.AsyncClient() as client:
#         resp = await client.post(
#             f"{GUAC_BASE_URL}/api/session/data/{DATA_SOURCE}/connections",
#             params={"token": token},
#             json=payload,
#             timeout=10,
#         )

#     if resp.status_code not in (200, 201):
#         raise GuacamoleError(f"Failed to create connection: {resp.status_code} {resp.text}")

#     return resp.json()["identifier"]


# # ─────────────────────────────────────────────────────────────
# # BUILD CLIENT URL
# # The Guacamole client URL encodes the connection as base64:
# #   base64( connection_id + "\x00" + "c" + "\x00" + data_source )
# # ─────────────────────────────────────────────────────────────
# # def _build_client_url(token: str, connection_id: str) -> str:
# #     raw = f"{connection_id}\x00c\x00{DATA_SOURCE}"
# #     client_id = base64.b64encode(raw.encode()).decode()
# #     # Return the full Guacamole URL — user lands on the remote desktop immediately
# #     return f"{GUAC_BASE_URL}/#/client/{client_id}?token={token}"



# def _build_client_url(token: str, connection_id: str) -> str:
#     raw = f"{connection_id}\x00c\x00{DATA_SOURCE}"
#     client_id = base64.b64encode(raw.encode()).decode()
#     return f"{GUAC_PUBLIC_URL}/#/client/{client_id}?token={token}"


# # ─────────────────────────────────────────────────────────────
# # PUBLIC ENTRY POINT
# # Called by the /vps/accounts/{id}/launch route
# # ─────────────────────────────────────────────────────────────
# async def get_launch_url(
#     vps_host: str,
#     vps_username: str,
#     vps_password: str,
#     protocol: str = "ssh",   # "ssh" for Linux, "rdp" for Windows
#     port: Optional[int] = None,
# ) -> str:
#     """
#     Ensure a Guacamole connection exists for this VPS and return
#     a browser-ready URL the user can open in a new tab.
#     """
#     if port is None:
#         port = 22 if protocol == "ssh" else 3389

#     # Stable connection name — one per VPS host+user combo
#     conn_name = f"HedgeBridge | {vps_host} ({vps_username})"

#     token = await _get_token()

#     # Re-use existing connection if already created
#     conn_id = await _find_connection(token, conn_name)

#     if not conn_id:
#         conn_id = await _create_connection(
#             token=token,
#             name=conn_name,
#             protocol=protocol,
#             hostname=vps_host,
#             port=port,
#             username=vps_username,
#             password=vps_password,
#         )

#     return _build_client_url(token, conn_id)