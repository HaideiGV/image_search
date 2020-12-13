import sys
import logging

import asyncio
from elasticsearch import AsyncElasticsearch
from aiohttp import web

sys.path.append(".")
from utils.es import recreate_images_index
from settings import IMAGES_INDEX


log = logging.getLogger(__name__)


es = AsyncElasticsearch()


async def images_search(request):
    search_term = request.match_info.get("search_term", "")
    query = {
        "query": {
            "bool": {
                "should": [
                    {"match": {"doc.camera": search_term}},
                    {"match": {"doc.author": search_term}},
                    {"match": {"doc.tags.keyword": search_term}},
                ]
            }
        }
    }

    data = await es.async_search.submit(body=query, index=IMAGES_INDEX)
    return web.json_response(data=data.get("response", {}).get("hits", []))


async def add_routes(app):
    app.router.add_get("/search/{search_term}", images_search)


async def start_background_tasks(app):
    app["recreate_es_index"] = asyncio.create_task(recreate_images_index(app))


async def cleanup_background_tasks(app):
    app["recreate_es_index"].cancel()
    await app["recreate_es_index"]


app = web.Application()
app.on_startup.append(add_routes)
app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)
web.run_app(app, host="0.0.0.0", port=80)
