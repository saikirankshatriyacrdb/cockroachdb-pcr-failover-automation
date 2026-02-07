#!/usr/bin/env python3
"""
Seamless Failover Service - Runs as a service to provide connection management
Can be used as a sidecar or service that applications query for connection strings
"""
import time
import signal
import sys
import json
from http.server import HTTPServer, BaseHTTPRequestHandler
from src.connection_manager import ConnectionManager, ApplicationConnectionHelper
from src.config import Config


class ConnectionHandler(BaseHTTPRequestHandler):
    """HTTP handler for connection string API"""

    def __init__(self, *args, helper=None, **kwargs):
        self.helper = helper
        super().__init__(*args, **kwargs)

    def do_GET(self):
        """Handle GET requests for connection strings"""
        if self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "status": "healthy",
                "active_cluster": self.helper.manager.get_active_cluster()
            }
            self.wfile.write(json.dumps(response).encode())

        elif self.path == '/connection':
            # Get connection string
            username = self.headers.get('X-DB-Username')
            password = self.headers.get('X-DB-Password')

            if not username or not password:
                self.send_response(400)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {"error": "Username and password required (X-DB-Username, X-DB-Password headers)"}
                self.wfile.write(json.dumps(response).encode())
                return

            try:
                conn_str = self.helper.get_db_connection_string(username, password)
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {
                    "connection_string": conn_str,
                    "active_cluster": self.helper.manager.get_active_cluster(),
                    "primary_cluster": Config.PRIMARY_CLUSTER_ID,
                    "standby_cluster": Config.STANDBY_CLUSTER_ID
                }
                self.wfile.write(json.dumps(response).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                response = {"error": str(e)}
                self.wfile.write(json.dumps(response).encode())

        elif self.path == '/clusters':
            # Get cluster status
            manager = self.helper.manager
            primary_healthy = manager.check_cluster_health(Config.PRIMARY_CLUSTER_ID)
            standby_healthy = manager.check_cluster_health(Config.STANDBY_CLUSTER_ID)
            active = manager.get_active_cluster()

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {
                "primary": {
                    "cluster_id": Config.PRIMARY_CLUSTER_ID,
                    "healthy": primary_healthy,
                    "active": active == Config.PRIMARY_CLUSTER_ID,
                    "sql_dns": manager.get_cluster_sql_dns(Config.PRIMARY_CLUSTER_ID)
                },
                "standby": {
                    "cluster_id": Config.STANDBY_CLUSTER_ID,
                    "healthy": standby_healthy,
                    "active": active == Config.STANDBY_CLUSTER_ID,
                    "sql_dns": manager.get_cluster_sql_dns(Config.STANDBY_CLUSTER_ID)
                },
                "current_active": active
            }
            self.wfile.write(json.dumps(response, indent=2).encode())

        else:
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {"error": "Not found", "available_endpoints": ["/health", "/connection", "/clusters"]}
            self.wfile.write(json.dumps(response).encode())

    def log_message(self, format, *args):
        """Suppress default logging"""
        pass


class SeamlessFailoverService:
    """Service that provides connection management via HTTP API"""

    def __init__(self, port=8080):
        self.port = port
        self.helper = ApplicationConnectionHelper()
        self.running = True

        # Setup signal handlers
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

    def signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        print("\nShutting down seamless failover service...")
        self.running = False

    def run(self):
        """Run the HTTP service"""
        handler = lambda *args, **kwargs: ConnectionHandler(*args, helper=self.helper, **kwargs)
        server = HTTPServer(('0.0.0.0', self.port), handler)

        print("=" * 60)
        print("Seamless Failover Service")
        print("=" * 60)
        print(f"\nService running on http://0.0.0.0:{self.port}")
        print("\nEndpoints:")
        print(f"  GET /health - Service health check")
        print(f"  GET /connection - Get connection string (requires X-DB-Username, X-DB-Password headers)")
        print(f"  GET /clusters - Get cluster status")
        print("\nPress Ctrl+C to stop")
        print("=" * 60)

        try:
            while self.running:
                server.handle_request()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
            print("\nService stopped")
