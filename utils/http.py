import logging

from aiohttp import ClientSession

from settings import API_KEY, BASE_IMAGES_URL

log = logging.getLogger(__name__)


async def refresh_auth_token():
    auth_token = None
    async with ClientSession() as session:
        url = f"{BASE_IMAGES_URL}/auth"
        headers = {"Content-Type": "application/json"}
        data = {"apiKey": API_KEY}
        async with session.post(url, json=data, headers=headers) as response:
            if response.status == 200:
                data = await response.json()
                is_auth_success = data.get("auth")
                token = data.get("token")
                if is_auth_success:
                    auth_token = token
            else:
                log.error("Auth token refreshing was failed.")
            return auth_token


async def fetch_all_image_ids(auth_token):
    image_ids = []
    async with ClientSession() as session:
        has_more_pages = True
        page = 1
        while has_more_pages:
            url = f"{BASE_IMAGES_URL}/images/?page={page}"
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            }
            async with session.get(url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    has_more_pages = data.get("hasMore")
                    picture_ids = [item.get("id") for item in data.get("pictures", [])]
                    image_ids.extend(picture_ids)
                else:
                    log.error(f"Fetching page[{url}] was failed.")
                page += 1

        return image_ids


async def fetch_images_data(auth_token, image_ids):
    images_data = []
    async with ClientSession() as session:
        for image_id in image_ids:
            headers = {
                "Authorization": f"Bearer {auth_token}",
                "Content-Type": "application/json",
            }
            url = f"{BASE_IMAGES_URL}/images/{image_id}"
            async with session.get(url=url, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    images_data.append(data)
                else:
                    log.error(f"Fetching image data for [{url}] was failed.")

        return images_data
