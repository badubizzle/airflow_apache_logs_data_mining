import logging
import os

import dotenv

dotenv.load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))
print('Loaded environments')

logging.basicConfig()
logging.debug('Starting app')
