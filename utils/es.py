import logging

import asyncio
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from .http import (
    refresh_auth_token,
    fetch_all_image_ids,
    fetch_images_data,
)
from settings import ES_HOSTS, IMAGES_INDEX, IMAGES_INDEX_RECREATION_TIMEOUT


log = logging.getLogger(__name__)


es = AsyncElasticsearch(hosts=ES_HOSTS)


def create_image_index_doc(image_data):
    tags = []
    for item in image_data.get("tags", "").split(" "):
        if item:
            item = item.replace("#", "")
            tags.append(item)

    return {
        "_index": IMAGES_INDEX,
        "doc": {
            "_id": image_data.get("id"),
            "author": image_data.get("author"),
            "camera": image_data.get("camera"),
            "tags": tags,
            "cropped_picture": image_data.get("cropped_picture"),
            "full_picture": image_data.get("full_picture"),
        },
    }


async def recreate_images_index(app):
    while True:
        log.info("Started process off recreating image index.")
        auth_token = await refresh_auth_token()

        if auth_token:
            image_ids = await fetch_all_image_ids(auth_token)
            images_data = await fetch_images_data(auth_token, image_ids)

            docs = []
            for image_data in images_data:
                doc = create_image_index_doc(image_data)
                docs.append(doc)

            await es.indices.delete(index=IMAGES_INDEX, ignore_unavailable=True)
            await es.indices.create(index=IMAGES_INDEX)
            await async_bulk(es, docs)
        else:
            log.error("Auth token was not fetched. Image index was not updated.")

        log.info("Finished process off recreating image index.")
        await asyncio.sleep(IMAGES_INDEX_RECREATION_TIMEOUT)
