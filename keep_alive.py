from aiohttp import web
import logging

logger = logging.getLogger(__name__)

async def handle(request):
    return web.Response(text="I'm alive!")

def create_keep_alive_server(port):
    app = web.Application()
    app.router.add_get('/', handle)
    app.router.add_get('/health', handle)
    logger.info(f"Keep-alive server created on port {port}")
    return app