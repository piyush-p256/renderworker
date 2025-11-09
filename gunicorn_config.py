# Gunicorn configuration for TeleStore Render Worker
# Optimized for large file uploads (up to 2GB)

import multiprocessing
import os

# Server socket
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

# Worker processes
workers = 2
worker_class = 'sync'
threads = 1

# Timeout settings
# Reasonable timeout since uploads happen in background threads
# Requests return immediately and don't block the worker
timeout = 120  # 2 minutes (enough for chunk merging)
graceful_timeout = 60
keepalive = 5

# Logging
accesslog = '-'
errorlog = '-'
loglevel = 'info'

# Process naming
proc_name = 'telestore-worker'

# Server mechanics
daemon = False
pidfile = None
umask = 0
user = None
group = None
tmp_upload_dir = None

# SSL (if needed)
keyfile = None
certfile = None
