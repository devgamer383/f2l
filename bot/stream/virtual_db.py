import json
import time
import aiosqlite
from typing import Optional, List, Dict, Any

DB_PATH = "virtual_streams.db"

class VirtualBatchDB:
    def __init__(self, db_path: str = DB_PATH):
        self.db_path = db_path
        self._initialized = False

    async def init_db(self):
        if self._initialized:
            return
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                CREATE TABLE IF NOT EXISTS virtual_batches (
                    batch_id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    total_size INTEGER NOT NULL,
                    mime_type TEXT NOT NULL,
                    parts_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                )
            """)
            await db.commit()
        self._initialized = True

    async def save_batch(
        self,
        batch_id: str,
        filename: str,
        total_size: int,
        mime_type: str,
        parts: List[Dict[str, Any]]
    ):
        await self.init_db()
        parts_json = json.dumps(parts)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("""
                INSERT OR REPLACE INTO virtual_batches 
                (batch_id, filename, total_size, mime_type, parts_json, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (batch_id, filename, total_size, mime_type, parts_json, time.time()))
            await db.commit()

    async def get_batch(self, batch_id: str) -> Optional[Dict[str, Any]]:
        await self.init_db()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT batch_id, filename, total_size, mime_type, parts_json, created_at
                FROM virtual_batches WHERE batch_id = ?
            """, (batch_id,)) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "batch_id": row[0],
                    "filename": row[1],
                    "total_size": row[2],
                    "mime_type": row[3],
                    "parts": json.loads(row[4]),
                    "created_at": row[5]
                }

virtual_db = VirtualBatchDB()
