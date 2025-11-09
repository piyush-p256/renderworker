# Gunicorn configuration for TeleStore Render Worker
# Optimized for large file uploads (up to 2GB)

import multiprocessing
import os

# Server socket
bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

# Worker processes
workers = 2
worker_class = 'uvicorn.workers.UvicornWorker'  # Use Uvicorn worker for async support
threads = 1

# Timeout settings
# With async streaming, workers don't timeout during downloads
# The async generator yields chunks without blocking
timeout = 600  # 10 minutes (generous timeout, but shouldn't hit it with async)
graceful_timeout = 120
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
