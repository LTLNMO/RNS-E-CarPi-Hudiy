from http.server import HTTPServer, SimpleHTTPRequestHandler
import json
import os

# Always serve files from the folder this script lives in
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.chdir(BASE_DIR)

class MockServer(SimpleHTTPRequestHandler):

    def do_GET(self):
        if self.path == "/api/config":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            try:
                with open("config.json", "r") as f:
                    self.wfile.write(f.read().encode())
            except FileNotFoundError:
                self.wfile.write(json.dumps({"platform": "TT", "features": {}}).encode())
            return

        if self.path == "/api/timezones":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(["UTC", "Europe/Berlin", "America/New_York"]).encode())
            return

        if self.path in ("/", ""):
            self.path = "/index.html"

        return super().do_GET()

    def do_POST(self):
        if self.path == "/api/config":
            length = int(self.headers.get("Content-Length", 0))
            data = self.rfile.read(length)
            with open("config.json", "wb") as f:
                f.write(data)
            self.send_response(200)
            self.end_headers()

print("Server running at http://localhost:8000")
HTTPServer(("", 8000), MockServer).serve_forever()
