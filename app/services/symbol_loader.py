from sqlalchemy import select
from app.models.all_models import Symbol


async def load_symbols(session):
    result = await session.execute(select(Symbol).where(Symbol.is_active == True))
    return result.scalars().all()