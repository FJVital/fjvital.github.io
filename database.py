import os
import sqlite3

# Simple user "database" for authentication
# In a real app, this would be a proper SQL table
USERS = {
    "fjvital@gmail.com": {
        "username": "fjvital@gmail.com",
        "hashed_password": "$2b$12$EixZaYVK1fsbw1ZfbX3OXePaWxn96p36WQoeG6L6s5Wr7Hn/hA2.u" # This is $Fortune11$
    }
}

def get_user(username: str):
    """
    Looks up a user in our 'database' dictionary.
    """
    if username in USERS:
        return USERS[username]
    return None

# The rest of your existing database functions (for CSV tracking) go here
def init_db():
    # If you have specific SQL setup code, it stays here
    pass