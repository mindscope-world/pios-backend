from fastapi.middleware.gzip import GZipMiddleware


class ExcludePathsGZipMiddleware:
    """
    Wraps Starlette's GZipMiddleware but bypasses it entirely for a fixed set
    of paths.

    GZipMiddleware buffers response bytes until `minimum_size` accumulates
    before flushing anything (see starlette.middleware.gzip.GZipResponder).
    Real browsers always send `Accept-Encoding: gzip`, so a slow-trickling
    streaming response (SSE) whose individual chunks are smaller than
    `minimum_size` never gets flushed to a real browser client at all --
    verified live: `curl --compressed .../intelligence/stream` hangs
    indefinitely, while plain `curl` (no Accept-Encoding) streams immediately.
    Excluding the SSE routes from gzip is the fix rather than lowering
    `minimum_size`, since Starlette's GZipMiddleware has no per-route or
    per-content-type exclusion option of its own.
    """

    def __init__(self, app, excluded_paths: set[str], **gzip_kwargs):
        self._excluded_paths = excluded_paths
        self._gzip_app = GZipMiddleware(app, **gzip_kwargs)
        self._plain_app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http" and scope["path"] in self._excluded_paths:
            await self._plain_app(scope, receive, send)
        else:
            await self._gzip_app(scope, receive, send)
