from me_models import State, Queue, Db_connect
import utils
import datetime
import json
from flask import Flask
from flask_cors import CORS
import mongoengine as me

cfg = utils.config()
Db_connect()

app = Flask(__name__)
CORS(app)


@app.route("/")
def hello():
    return "Hello World!"


@app.route("/state")
def state():
    response = json.loads(State.objects().get().to_json())
    response.pop("_id")
    counts = {
        "total_files": Queue.objects().count(),
        "MD5sum_done": Queue.objects(md5sum__ne=None).count(),
        "in_gphotos": Queue.objects(in_gphotos=True).count(),
        "mirrored": Queue.objects(mirrored=True).count(),
        "purged": Queue.objects(purged=True).count(),
    }
    response.update(counts)
    return json.dumps(response)
