FROM python:3.12-slim

# pinned: `latest` makes the resolver a moving input, so two builds of one commit can
# install different dependency versions
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /bin/

WORKDIR /app

# deps install from the lock before the source is copied, so an api/ edit is a cache hit.
# --frozen fails rather than re-resolving, so the image can never ship a dependency set
# the lockfile does not describe. the download cache lives in a build mount rather than
# the image, which is what keeps a lock change cheap without paying for it in image size
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv export --frozen --no-dev --no-emit-project -o /tmp/requirements.txt \
    && uv pip install --system -r /tmp/requirements.txt \
    && rm /tmp/requirements.txt

COPY api/ .

ENV PORT=8080

# drop root: the service only reads its code and the read-only datasets ConfigMap mount
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && chown -R appuser:appuser /app
USER 10001

CMD ["python", "main.py"]
