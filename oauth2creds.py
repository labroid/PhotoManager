import json
import pydata_google_auth
import pydata_google_auth.cache

SCOPES = [
    "https://www.googleapis.com/auth/drive.metadata.readonly",
    "https://www.googleapis.com/auth/photoslibrary.appendonly",
]
CLIENT_SECRETS = "client_secrets_app.json"


def get_credentials(
    client_secrets=CLIENT_SECRETS, scopes=None, filename="credentials.json"
):
    scopes = scopes or SCOPES
    with open(client_secrets, "r") as fp:
        secrets = json.loads(fp.read())["installed"]
    return pydata_google_auth.get_user_credentials(
        scopes=scopes,
        client_id=secrets.get("client_id"),
        client_secret=secrets.get("client_secret"),
        auth_local_webserver=True,
        credentials_cache=pydata_google_auth.cache.ReadWriteCredentialsCache(
            filename=filename
        ),
    )
