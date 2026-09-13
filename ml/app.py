"""The sidecar's whole surface. Two endpoints, and a health check.

SIDECAR.md is the contract and this is the implementation of it; if the two
ever disagree, SIDECAR.md is what the archive was written against.

    GET  /models  ->  { "models": [ { "slug", "loaded", "bytes" } ] }
    POST /ocr     ->  { "text", "model", "ms" }        multipart: model, file

Nothing here imports paddle. The engine is injected, which is what lets the
wire format — multipart parsing, the JSON shapes, every failure path — be
tested with no weights on disk. The half that cannot be tested without a
gigabyte of models is then exactly one function call wide.

NOT AUTHENTICATED, ON PURPOSE
-----------------------------
There is no token on these routes and there must be no `ports:` on the service.
It is reachable from the archive container over the compose network and from
nothing else, which puts it in the same class as the ingest token rather than
in the class of things exposed to a browser. If this ever needs publishing,
that is a decision with a lock on it, not a flag.
"""
from __future__ import annotations

import os
import traceback

from flask import Flask, jsonify, request

from engine import Engine

app = Flask(__name__)

# Replaced wholesale in tests. A module-level singleton rather than a factory
# because the point of the object is that it holds one loaded model.
engine = Engine()

# Bigger than anything the archive will send — it refuses stills over its own
# per-kind cap long before this — but a bound rather than none, so a wrong
# caller cannot make this container read an arbitrary amount into memory.
app.config["MAX_CONTENT_LENGTH"] = int(os.environ.get("ML_MAX_BYTES", 64 << 20))


@app.get("/health")
def health():
    """Up, and nothing more. Deliberately does not touch the models: a health
    check that loads a gigabyte is a health check that times out."""
    return jsonify({"ok": True})


@app.get("/models")
def models():
    return jsonify({"models": engine.known()})


@app.post("/ocr")
def ocr():
    slug = (request.form.get("model") or "").strip()
    if not slug:
        return "model is required — the slug from the archive's slot", 400
    f = request.files.get("file")
    if f is None:
        return "file is required — the image bytes, not a path", 400
    data = f.read()
    if not data:
        return "the file part was empty", 400

    # The extension only tells the decoder what to expect; the archive has
    # already classified the file by its real format before it ever queued an
    # ocr job, so this is a hint and not a trust boundary.
    name = f.filename or "upload.png"
    suffix = os.path.splitext(name)[1].lower() or ".png"

    try:
        text, ms = engine.read(slug, data, suffix)
    except KeyError:
        # The archive asked for a slug this container does not serve. A 400,
        # not a 500: nothing here is broken, the request was for something that
        # does not exist, and the difference decides whether an admin goes
        # looking at the slot or at the container.
        return f"this sidecar does not serve {slug!r}", 400
    except Exception as e:                                  # noqa: BLE001
        # The archive puts this body on the job as the reason, so it has to
        # read as a sentence rather than as a stack trace. The trace goes to
        # the container log, where somebody debugging it can find it.
        app.logger.error("ocr failed for %s: %s", slug, traceback.format_exc())
        return f"{type(e).__name__}: {e}", 500

    # `text` is kept with its line breaks. The archive splits on them and
    # writes one snippet_line per line — see runOcr — so joining them here
    # would flatten a meme's two-line caption into one line nobody can
    # re-split.
    return jsonify({"text": text, "model": slug, "ms": ms})


if __name__ == "__main__":
    # waitress, not app.run(): the Flask development server says so on every
    # boot and is not meant to hold a connection for the two minutes a first
    # load can take. Two threads, because the archive runs one OCR at a time
    # and the second thread is there so /models and /health still answer while
    # a picture is being read.
    from waitress import serve
    serve(app, host="0.0.0.0", port=int(os.environ.get("PORT", 3003)), threads=2)
