from http.server import BaseHTTPRequestHandler, HTTPServer
import json
from apache_beam.yaml import main

class ValidationHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path == '/validate':
            content_length = int(self.headers['Content-Length'])
            payload = json.loads(self.rfile.read(content_length).decode('utf-8'))

            yaml_spec = payload.get('yamlPipeline')
            project = payload.get('project')
            region = payload.get('region')

            try:
                main.run(['--yaml_pipeline', yaml_spec,
                          '--runner', 'apache_beam.runners.dataflow.DataflowRunner',
                          '--dry_run', 'True',
                          # The temp_location parameter is required but is unused when dry_run is True
                          '--temp_location', 'gs://',
                          '--project', project,
                          '--region', region])
            except Exception as e:
                print('Pipeline validation failed')
                print(e)
                self.send_response(400)
                self.send_header('Content-type', 'text/plain')
                self.end_headers()
                self.wfile.write(str(e).encode('utf-8'))
                return
            print('Pipeline validation succeeded!\n')
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'')
        else:
            self.send_response(404)
            self.end_headers()

    def log_message(self, format, *args):
        pass  # Do nothing to suppress logging.

if __name__ == '__main__':
    server_address = ('',7845)
    httpd = HTTPServer(server_address, ValidationHandler)
    print('Started Dataflow job builder validation server!')
    httpd.serve_forever()
