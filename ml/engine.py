"""PaddleOCR, loaded when something first asks for it.

Everything that talks to PaddleOCR is in this file and nothing else imports it,
so the HTTP contract in app.py can be tested against a stub. That split is not
tidiness: the wire format is the part two containers have to agree on, and it
should be verifiable without a gigabyte of model weights present.

WHY THE MODEL NAMES ARE WRITTEN OUT
-----------------------------------
The obvious way to construct this is `PaddleOCR(lang="ch")`, and it is wrong
for our purpose. In paddleocr 3.x that resolves to:

    lang in ("ch", "chinese_cht", "japan")  ->  PP-OCRv5_server_det
                                                PP-OCRv5_server_rec

— the SERVER pair, several times the weights and several times the work per
picture, on a two-core NAS. The slot in the admin panel would have said
`PP-OCRv5_mobile` while the container quietly ran something else, which is the
kind of gap nobody finds because nothing looks broken.

So the pair is named explicitly and `lang` is never passed. `PP-OCRv5_mobile_rec`
is the default multi-script recogniser: Simplified Chinese, Traditional Chinese,
Pinyin, English and Japanese in one model. The per-language models
(korean_, latin_, cyrillic_, th_, …) exist for scripts it does NOT cover, and
Japanese is not among them — which is the whole reason one slot can serve this
archive.

WHERE THE WEIGHTS LIVE
----------------------
`PADDLE_PDX_CACHE_HOME` (read at import time by paddlex) points at /models, and
downloads land in `<cache>/official_models/<model name>/`. That directory is the
volume, so first use downloads and every later start finds them already there.
Nothing is baked into the image: the models are data, the image is code, and
swapping a model must not mean rebuilding the thing that serves pages.
"""
from __future__ import annotations

import os
import threading
import time
from pathlib import Path

# The slugs the archive's MODEL_CATALOG offers for the `ocr` task, mapped to the
# two models each one actually needs. Keep in step with MODEL_CATALOG in
# archive.js — the archive proposes a slug, this decides whether it means
# anything.
PAIRS: dict[str, tuple[str, str]] = {
    "PP-OCRv5_mobile": ("PP-OCRv5_mobile_det", "PP-OCRv5_mobile_rec"),
    "PP-OCRv5_server": ("PP-OCRv5_server_det", "PP-OCRv5_server_rec"),
}

CACHE = Path(os.environ.get("PADDLE_PDX_CACHE_HOME", "/models"))
OFFICIAL = CACHE / "official_models"


def _bytes(path: Path) -> int:
    """Every regular file under a directory. 0 when it is not there yet."""
    if not path.is_dir():
        return 0
    total = 0
    for p in path.rglob("*"):
        try:
            if p.is_file():
                total += p.stat().st_size
        except OSError:
            # A download in flight can delete a temp file between the walk and
            # the stat. A size that is briefly low beats a 500 on /models.
            pass
    return total


class Engine:
    """One PaddleOCR pipeline at a time, built on first use.

    Serialised behind a lock. PaddleOCR's predictors are not documented as
    thread-safe and the archive's worker only ever has one OCR job in flight
    (claimNormalize() ?? claimOcr() ?? claimTranscribe(), one at a time), so
    parallelism here would buy nothing and risk a class of bug that is
    miserable to reproduce.
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._slug: str | None = None
        self._ocr = None

    # ---- what /models answers ---------------------------------------------
    def known(self) -> list[dict]:
        """Every slug this container can serve, with what is on disk for it.

        `loaded` means in memory and able to answer now; a model can be fully
        downloaded and not loaded, which is why the Admin card draws the two
        differently. Reported for every slug rather than only the configured
        one, so switching the slot is a decision you can make from the panel
        with the sizes in front of you.
        """
        out = []
        for slug, (det, rec) in PAIRS.items():
            dirs = [OFFICIAL / det, OFFICIAL / rec]
            size = sum(_bytes(d) for d in dirs)
            out.append({
                "slug": slug,
                "loaded": self._slug == slug and self._ocr is not None,
                "bytes": size,
                # Not part of the contract the archive requires; useful when
                # somebody curls this by hand to find out why a slug is 0 bytes.
                "present": all(d.is_dir() for d in dirs),
                "models": [det, rec],
            })
        return out

    # ---- and what /ocr does -----------------------------------------------
    def read(self, slug: str, data: bytes, suffix: str = ".png") -> tuple[str, int]:
        """The text on one image, and how long it took in ms.

        Raises KeyError for a slug this container does not serve — app.py turns
        that into a 400 rather than a 500, because an unknown slug is the
        archive asking for something that does not exist rather than this
        container failing.
        """
        if slug not in PAIRS:
            raise KeyError(slug)
        t0 = time.monotonic()
        with self._lock:
            ocr = self._pipeline(slug)
            # A path, not bytes: predict() accepts a filename or an ndarray, and
            # decoding to an ndarray here would mean importing cv2/numpy in this
            # process for no gain. The temp file never leaves this container and
            # is removed whatever happens.
            import tempfile
            fd, tmp = tempfile.mkstemp(suffix=suffix)
            try:
                with os.fdopen(fd, "wb") as f:
                    f.write(data)
                results = ocr.predict(tmp)
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
        lines: list[str] = []
        for res in results or []:
            # dict-like in 3.x; .get keeps this working if a future version
            # returns an object that still supports mapping access.
            for t in (res.get("rec_texts") or []):
                t = (t or "").strip()
                if t:
                    lines.append(t)
        return "\n".join(lines), int((time.monotonic() - t0) * 1000)

    def _pipeline(self, slug: str):
        """Build it, or hand back the one already built. Call under the lock."""
        if self._slug == slug and self._ocr is not None:
            return self._ocr
        det, rec = PAIRS[slug]
        # Imported here and not at module scope: `import paddleocr` pulls in
        # paddle and costs seconds, and /health and /models must answer while
        # nothing has been asked of the models yet.
        from paddleocr import PaddleOCR
        self._ocr = PaddleOCR(
            text_detection_model_name=det,
            text_recognition_model_name=rec,
            # All three off. They are document-scanner stages — deskewing a
            # photographed page, rotating a sideways scan — and a meme is a
            # screenshot that is already the right way up. Each one is another
            # model to download and another pass per picture.
            use_doc_orientation_classify=False,
            use_doc_unwarping=False,
            use_textline_orientation=False,
        )
        self._slug = slug
        return self._ocr

    def warm(self, slug: str) -> dict:
        """Download and load a slug without reading anything.

        Exists so a first run can be triggered deliberately, watched, and timed
        — rather than discovered by uploading a picture and waiting to see
        whether it worked. Nothing in the archive calls this: /models is a
        status probe by design and must stay fast, so warming is a decision
        somebody makes at a shell.
        """
        if slug not in PAIRS:
            raise KeyError(slug)
        t0 = time.monotonic()
        with self._lock:
            self._pipeline(slug)
        det, rec = PAIRS[slug]
        return {"slug": slug, "ms": int((time.monotonic() - t0) * 1000),
                "bytes": sum(_bytes(OFFICIAL / d) for d in (det, rec))}

    def unload(self) -> None:
        """Drop it. Nothing calls this yet; it is here because a slot change
        should be able to free the old weights without restarting the
        container, and leaving the hook out is how that becomes a restart."""
        with self._lock:
            self._ocr = None
            self._slug = None
