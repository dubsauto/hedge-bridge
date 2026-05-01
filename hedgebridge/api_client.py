# hedgebridge/api_client.py

import os
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi

load_dotenv()

API_TOKEN = os.getenv("ACCESS_TOKEN")

_metaapi_client: MetaApi | None = None


def get_metaapi_client() -> MetaApi:
    global _metaapi_client

    if _metaapi_client is None:
        if not API_TOKEN:
            raise ValueError("❌ ACCESS_TOKEN is not set in environment")

        print("🚀 Initializing MetaApi client...")
        _metaapi_client = MetaApi(API_TOKEN)

    return _metaapi_client


def reset_metaapi_client() -> MetaApi:
    """Force a fresh MetaApi client — call this when the SDK has zombie state."""
    global _metaapi_client
    print("🔄 Resetting MetaApi client singleton...")
    _metaapi_client = None
    return get_metaapi_client()