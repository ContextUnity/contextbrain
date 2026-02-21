import asyncio

from .service import serve

if __name__ == "__main__":
    # .env loaded in serve() via Config.load() (single config entry)
    asyncio.run(serve())
