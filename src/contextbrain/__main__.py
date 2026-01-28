import asyncio

from .service import serve

if __name__ == "__main__":
    from dotenv import load_dotenv

    load_dotenv()
    asyncio.run(serve())
