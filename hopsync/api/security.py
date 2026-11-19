"""Request checks that run before anything else, for HTTP and WebSocket."""

import secrets
from http.cookies import SimpleCookie

from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse

COOKIE_NAME = "hopsync_session"
UNSAFE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}

HINT_PAGE = (
    "<!doctype html><title>hopsync</title>"
    "<p>Not signed in. Open the link printed in the terminal where hopsync is running.</p>"
)


def same_secret(a: str, b: str) -> bool:
    """Constant-time comparison (also safe for non-ASCII input)."""
    return secrets.compare_digest(a.encode("utf-8"), b.encode("utf-8"))


class SecurityMiddleware:
    """Pure ASGI middleware: it also sees WebSocket connections, which the
    usual HTTP-only middleware classes do not."""

    def __init__(self, app, *, port: int, session_secret: str) -> None:
        self.app = app
        self.hosts = {f"127.0.0.1:{port}", f"localhost:{port}"}
        self.origins = {f"http://{host}" for host in self.hosts}
        self.session_secret = session_secret

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            await self.check_http(scope, receive, send)
        elif scope["type"] == "websocket":
            await self.check_websocket(scope, receive, send)
        else:
            await self.app(scope, receive, send)

    # ---- helpers -------------------------------------------------------

    @staticmethod
    def headers(scope) -> dict:
        return {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}

    def has_session(self, headers: dict) -> bool:
        cookie = SimpleCookie()
        try:
            cookie.load(headers.get("cookie", ""))
        except Exception:  # a malformed Cookie header simply means "not signed in"
            return False
        morsel = cookie.get(COOKIE_NAME)
        return morsel is not None and same_secret(morsel.value, self.session_secret)

    # ---- HTTP ----------------------------------------------------------

    async def check_http(self, scope, receive, send):
        headers = self.headers(scope)
        # a. Host check: stops DNS rebinding (a hostile site whose name
        #    resolves to 127.0.0.1 would send its own name as Host).
        if headers.get("host") not in self.hosts:
            return await self.reply(PlainTextResponse("Bad Host header", 400), scope, receive, send)
        # b. Everything except /auth needs the session cookie.
        if scope["path"] != "/auth" and not self.has_session(headers):
            if scope["path"] == "/" and scope["method"] == "GET":
                response = HTMLResponse(HINT_PAGE, 401)
            else:
                response = JSONResponse({"error": "unauthorized", "message": "not signed in"}, 401)
            return await self.reply(response, scope, receive, send)
        # c. State-changing requests: a browser always sends Origin on
        #    cross-site POSTs, so a foreign one is refused (on top of SameSite=Strict).
        origin = headers.get("origin")
        if scope["method"] in UNSAFE_METHODS and origin is not None and origin not in self.origins:
            response = JSONResponse({"error": "forbidden", "message": "bad Origin"}, 403)
            return await self.reply(response, scope, receive, send)
        await self.app(scope, receive, send)

    @staticmethod
    async def reply(response, scope, receive, send):
        await response(scope, receive, send)

    # ---- WebSocket -----------------------------------------------------

    async def check_websocket(self, scope, receive, send):
        headers = self.headers(scope)
        # d. CORS does not apply to WebSockets: any page may open one to
        #    127.0.0.1, so we check Origin ourselves. It is REQUIRED here.
        ok = (
            headers.get("host") in self.hosts
            and headers.get("origin") in self.origins
            and self.has_session(headers)
        )
        if not ok:
            await send({"type": "websocket.close", "code": 1008})  # policy violation
            return
        await self.app(scope, receive, send)
