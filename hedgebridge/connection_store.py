#hedgebridge/connection_store.py

_connections = {}

def set_connection(account_id, connection):
    _connections[account_id] = connection

def get_connection(account_id):
    return _connections.get(account_id)

def remove_connection(account_id):
    _connections.pop(account_id, None)


def get_all_connections():
    return _connections