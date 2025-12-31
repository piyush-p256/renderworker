import os

bind = f"0.0.0.0:{os.environ.get('PORT', '10000')}"

worker_class = "uvicorn.workers.UvicornWorker"
workers = 1   

timeout = 600
graceful_timeout = 120
keepalive = 5

accesslog = "-"
errorlog = "-"
loglevel = "info"

proc_name = "telestore-worker"
