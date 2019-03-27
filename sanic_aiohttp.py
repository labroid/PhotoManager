from sanic import Sanic
from sanic import response
import aiohttp
import sys


from oauth2creds import get_credentials

credentials=get_credentials()

app = Sanic(__name__)


async def fetch(session, url):
    """
    Use session object to perform 'get' request on url
    """
    creds = get_credentials()

    headers = {
        "Authorization": f"Bearer {creds.token}"
    }
    async with session.get(url, headers=headers) as result:
        return await result.json()


@app.route('/')
async def handle_request(request):
    url = r"https://www.googleapis.com/drive/v3/files/root"
    async with aiohttp.ClientSession() as session:
        result = await fetch(session, url)
        return response.json(result)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=8000, workers=1)