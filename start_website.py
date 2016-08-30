#!/usr/bin/python3
from web.web import app
import optparse

if __name__ == '__main__':
    default_ip="127.0.0.1"
    default_port="5000"
    parser = optparse.OptionParser()
    parser.add_option("-i", "--ip",
                      help="IP address (use 0.0.0.0 for all) [default %s]" % default_ip, default=default_ip)
    parser.add_option("-p", "--port",
                      help="Port to listen to [default %s]" % default_port, default=default_port)
    parser.add_option("-d", "--debug",
                      action="store_true", dest="debug",
                      help="Enable debug mode")

    options, _ = parser.parse_args()

    app.run(
        debug=options.debug,
        host=options.ip,
        port=int(options.port),
        threaded=True
    )
