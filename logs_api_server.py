from waitress import serve
import app
import logging

logging.basicConfig(filename="error.log",
                            filemode='a',
                            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                            datefmt='%H:%M:%S',
                            level=logging.DEBUG)

logger = logging.getLogger('waitress')

serve(app.app, host='0.0.0.0', port=9980)