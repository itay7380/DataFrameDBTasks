from waitress import serve
import threading
from flask import Flask
from base.web_server.api.sum_values import blueprints


_SERVER_CONNECTION = ["0.0.0.0", 5000]


class CWebServer:
    def __init__(self, sAppName: str):
        self._app = Flask(sAppName)
        for bp in blueprints:
            self._app.register_blueprint(bp)

    def Start(self):
        ThreadServer = threading.Thread(target=self._StartServer, daemon=True)
        ThreadServer.start()

    def _StartServer(self):
        serve(
            self._app,
            host=_SERVER_CONNECTION[0],
            port=_SERVER_CONNECTION[1],
        )
