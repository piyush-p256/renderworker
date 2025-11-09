# Gunicorn configuration for TeleStore Worker (optimized for large file uploads)

import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"
workers = 2
worker_class = 'sync'
threads = 1
timeout = 1800  # 30 minutes
graceful_timeout = 60
keepalive = 5
accesslog = '-'
errorlog = '-'
loglevel = 'info'
proc_name = 'telestore-worker'
