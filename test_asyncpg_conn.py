# test_asyncpg_connection.py
import asyncio
import asyncpg
import os

async def connect_test():
    # Изменяем схему для asyncpg.connect()
    # Удаляем '+asyncpg'
    db_url_alembic = os.getenv("ALEMBIC_DATABASE_URL", "postgresql+asyncpg://postgres:postgres@localhost:5435/task_orchestrator")
    
    # Заменяем 'postgresql+asyncpg' на 'postgresql'
    db_url_for_asyncpg = db_url_alembic.replace("postgresql+asyncpg://", "postgresql://")

    print(f"Attempting to connect using URL: {db_url_for_asyncpg}")
    try:
        conn = await asyncpg.connect(db_url_for_asyncpg) # Используем измененный URL
        print("Successfully connected to PostgreSQL!")
        await conn.close()
    except Exception as e:
        print(f"Failed to connect: {e}")

if __name__ == "__main__":
    asyncio.run(connect_test())