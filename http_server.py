from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import urlparse, parse_qs

import json
import subprocess


def run_job(request_handler: BaseHTTPRequestHandler, script_path: str, job_name: str):
    try:
        # Construct the command to run the shell script with the -j parameter
        command = [script_path, '-j', job_name]

        # Run the shell script
        result = subprocess.run(command, capture_output=True, text=True)


        request_handler.send_response(200)
        request_handler.send_header('Content-type', 'application/json')
        request_handler.end_headers()
        response = {
            "return_code": result.returncode,
            "terminal_output": result.stdout
        }
        request_handler.wfile.write(json.dumps(response).encode())
    except Exception as e:
        request_handler.send_response(500)
        request_handler.send_header('Content-type', 'application/json')
        request_handler.end_headers()
        response = {
            "error": "There was an internal error"
        }
        request_handler.wfile.write(json.dumps(response).encode())     

class SimpleAPIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        parsed_url = urlparse(self.path)
        query_params = parse_qs(parsed_url.query)
        
        # Define a basic API endpoint
        if (parsed_url.path == '/api/hello'):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'message': 'Hello, world!'}
            self.wfile.write(json.dumps(response).encode())
        elif (parsed_url.path == '/api/run_job'):
            run_job(self, "/home/danielbaquini/spark-submiter.sh", query_params["job_name"][0])
        else:
            # Handle 404 Not Found
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Endpoint not found')

    def do_POST(self):
        if self.path == '/api/echo':
            # Read the content length
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            # Echo the data back in JSON format
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            response = {'received': post_data.decode()}
            self.wfile.write(json.dumps(response).encode())
        else:
            # Handle 404 Not Found
            self.send_response(404)
            self.end_headers()
            self.wfile.write(b'Endpoint not found')

def run(server_class=HTTPServer, handler_class=SimpleAPIHandler, port=8000):
    server_address = ('', port)
    httpd = server_class(server_address, handler_class)
    print(f'Starting server on port {port}')
    httpd.serve_forever()

if __name__ == '__main__':
    run()
