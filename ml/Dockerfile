# The ML sidecar. Python, because PaddleOCR is.
#
# NOT alpine. paddlepaddle publishes manylinux wheels against glibc and there
# is no musl wheel, so alpine means compiling paddle from source — hours, and a
# rebuild you would come to dread. python:3.12-slim is Debian, the wheels
# install in seconds, and the size difference is noise next to the runtime
# itself.
#
# 3.12 and not 3.13: paddleocr's own metadata tops out at 3.12 for this
# version. Pinning the minor here rather than tracking `latest` means a DSM
# rebuild months from now produces the same runtime.
FROM python:3.12-slim

# libgomp1 for paddle's OpenMP, and the two libs OpenCV wants for image
# decoding. Installed explicitly because the failure without them is an
# ImportError deep inside a transitive dependency, which reads as a broken
# build rather than a missing package.
RUN apt-get update && apt-get install -y --no-install-recommends \
      libgomp1 libgl1 libglib2.0-0 \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copied first and separately so the long pip install is cached against
# dependency changes rather than every edit to app.py.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py engine.py ./

# NO MODELS IN THE IMAGE, deliberately. The weights land on the /models volume
# the first time something asks for a slug, and every later start finds them
# already there. That is what makes the admin panel's "load on demand" true,
# and it means swapping a model is a download rather than an image rebuild.
#
# PADDLE_PDX_CACHE_HOME is read by paddlex at import time; downloads go to
# <cache>/official_models/<model name>/, which is what engine.py walks to
# report real sizes to the Admin card.
# HOME is set because compose runs this as the archive's uid (1027), which has
# no home directory in this image. Nothing on the download path needs one —
# both the model cache and its lock directory land under PADDLE_PDX_CACHE_HOME,
# which is the volume — but a library that wants somewhere to put a font cache
# should find a writable path rather than failing halfway through a first run.
ENV MODEL_DIR=/models \
    PADDLE_PDX_CACHE_HOME=/models \
    HOME=/tmp \
    PORT=3003 \
    PYTHONUNBUFFERED=1

# Two cores on a DS923+, and one of them should still be able to answer a
# request while the other reads a picture. Left as an env var so it can be
# raised on better hardware without a rebuild.
ENV PADDLE_PDX_CPU_NUM_THREADS=2

# oneDNN OFF, and this one is not a tuning choice — it is the difference
# between reading pictures and not.
#
# paddlex picks a run mode per model with get_default_run_mode(), which returns
# "mkldnn" for any CPU model that is not on its MKLDNN_BLOCKLIST. Neither
# PP-OCRv5_mobile_det nor _rec is on that list, so both ran through oneDNN, and
# this paddle build cannot convert one of their attributes on that path:
#
#   NotImplementedError: (Unimplemented) ConvertPirAttribute2RuntimeAttribute
#   not support [pir::ArrayAttribute<pir::DoubleAttribute>]
#     (at .../new_executor/instruction/onednn/onednn_instruction.cc:116)
#
# Every picture failed identically, which is what identifies it as a backend
# fault rather than anything about the images. False here sends both models
# down the plain CPU path, which works.
#
# The cost is speed: oneDNN is the optimised kernel set, so expect a read to
# take noticeably longer than it would have. Still a fraction of a second per
# still on this hardware, and a slow answer beats an exception. Set it back to
# True to retest after a paddle upgrade — if PP-OCRv5 appears on the blocklist
# upstream, or the conversion is implemented, this line can go.
ENV PADDLE_PDX_ENABLE_MKLDNN_BYDEFAULT=False

# No USER here. Compose sets `user:` to the SAME uid the archive runs as, so
# there is one identity writing under /volume2/docker/ragekitsu and not two —
# see SIDECAR.md. Left to compose rather than baked in because the uid is a
# fact about that NAS, not about this image.

EXPOSE 3003

# No tini: this holds no child processes and no WAL-mode database, so there is
# nothing to reap and nothing that breaks on an unclean stop. The worst a hard
# kill costs is a partial model download, which the next start re-fetches.
CMD ["python", "app.py"]
