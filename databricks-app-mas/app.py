#!/usr/bin/env python3
"""
Ultra-Simple Python HTTP Server for Databricks
"""

import http.server
import socketserver
import os

PORT = 8501

class SimpleHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/':
            self.send_response(200)
            self.send_header('Content-type', 'text/html')
            self.end_headers()
            
            html = """
            <html>
            <head><title>Multi-Agent Supervisor Chat</title></head>
            <body>
                <h1>🤖 Multi-Agent Supervisor Chat</h1>
                <p>✅ App is running successfully!</p>
                <p>This confirms the deployment is working.</p>
                <hr>
                <p><strong>Configuration:</strong></p>
                <ul>
                    <li>MAS Endpoint: mas-6c04fa76-endpoint</li>
                    <li>Host: https://e2-demo-field-eng.cloud.databricks.com</li>
                    <li>Workspace ID: 1444828305810485</li>
                </ul>
            </body>
            </html>
            """
            
            self.wfile.write(html.encode())
        else:
            self.send_response(404)
            self.end_headers()

if __name__ == '__main__':
    with socketserver.TCPServer(("", PORT), SimpleHTTPRequestHandler) as httpd:
        print(f"Server running on port {PORT}")
        httpd.serve_forever()
