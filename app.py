from fastapi import FastAPI, Request, HTTPException, Form, File, UploadFile, BackgroundTasks
from pydantic import BaseModel
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import requests
import httpx
import httpx
import sys
import subprocess
import hmac
import hashlib
import json
import json
import base64
import time
import shutil
from typing import List, Optional
from datetime import datetime
import threading
from telethon import TelegramClient, custom, version, events
from telethon.sessions import StringSession
from telethon.tl.types import InputFileBig
from telethon.tl.functions.upload import SaveBigFilePartRequest
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
import mimetypes
import gdown
import uuid
import gdown
import uuid
import re
import concurrent.futures
import psutil
import io

from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.backends import default_backend
from cryptography.fernet import Fernet

# ========== RESOURCE MANAGER ==========

class ResourceManager:
    """Dynamically manages concurrency limits based on system resources"""
    
    def __init__(self):
        self.cpu_count = psutil.cpu_count(logical=True) or 4
        self.total_ram_gb = psutil.virtual_memory().total / (1024 ** 3)
        self.upload_semaphore_limit = None
        self.io_pool_limit = None
        self._calculate_limits()
        
    def _calculate_limits(self):
        """Calculate optimal concurrency limits based on current resources"""
        cpu_percent = psutil.cpu_percent(interval=0.1)
        mem = psutil.virtual_memory()
        available_ram_gb = mem.available / (1024 ** 3)
        
        # Base calculations
        upload_workers = min(
            self.cpu_count * 2,                      # 2 workers per CPU core
            int((available_ram_gb - 0.5) * 3),      # 3 workers per available GB (reserve 0.5GB)
            20                                       # Hard maximum
        )
        
        io_workers = min(
            self.cpu_count * 3,
            int((available_ram_gb - 0.5) * 4),
            15
        )
        
        # Adjust based on current CPU load
        if cpu_percent > 80:
            upload_workers = max(2, upload_workers // 2)  # Reduce by half, minimum 2
            io_workers = max(3, io_workers // 2)
        elif cpu_percent < 30:
            upload_workers = min(20, int(upload_workers * 1.2))  # Increase by 20%
            io_workers = min(15, int(io_workers * 1.2))
        
        # Safety minimums
        upload_workers = max(2, upload_workers)
        io_workers = max(3, io_workers)
        
        self.upload_semaphore_limit = upload_workers
        self.io_pool_limit = io_workers
        
    def get_limits(self):
        """Recalculate and return current limits"""
        self._calculate_limits()
        return {
            'upload_workers': self.upload_semaphore_limit,
            'io_workers': self.io_pool_limit,
            'cpu_count': self.cpu_count,
            'total_ram_gb': round(self.total_ram_gb, 2),
            'cpu_percent': psutil.cpu_percent(interval=0.1),
            'ram_percent': psutil.virtual_memory().percent
        }

# Initialize resource manager
resource_manager = ResourceManager()
initial_limits = resource_manager.get_limits()

print(f"\n🚀 Resource Manager Initialized:")
print(f"   CPU Cores: {initial_limits['cpu_count']}")
print(f"   Total RAM: {initial_limits['total_ram_gb']} GB")
print(f"   Upload Workers: {initial_limits['upload_workers']}")
print(f"   IO Workers: {initial_limits['io_workers']}")
print(f"   Current CPU: {initial_limits['cpu_percent']}%")
print(f"   Current RAM: {initial_limits['ram_percent']}%\n")

# Global ThreadPool for blocking I/O (dynamically sized)
io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=initial_limits['io_workers'])

# Global progress tracking
import_progress = {}
import_cancellation_flags = set()
active_subprocesses = {} # task_id -> Popen object

# Lifespan for cleanup
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    asyncio.create_task(listener_manager.start())
    asyncio.create_task(monitor_resources())
    asyncio.create_task(cleanup_stale_uploads())
    yield
    # Cleanup
    await listener_manager.stop()

async def monitor_resources():
    """Periodically monitor and log resource usage"""
    while True:
        await asyncio.sleep(300)  # Every 5 minutes
        try:
            limits = resource_manager.get_limits()
            print(f"\n📊 Resource Status:")
            print(f"   CPU: {limits['cpu_percent']}% | RAM: {limits['ram_percent']}%")
            print(f"   Upload Workers: {limits['upload_workers']} | IO Workers: {limits['io_workers']}")
            
            # Update io_pool if needed (recreate with new limit)
            global io_pool
            if io_pool._max_workers != limits['io_workers']:
                print(f"   ⚙️  Adjusting IO pool: {io_pool._max_workers} -> {limits['io_workers']}")
                old_pool = io_pool
                io_pool = concurrent.futures.ThreadPoolExecutor(max_workers=limits['io_workers'])
                old_pool.shutdown(wait=False)
        except Exception as e:
            print(f"Resource monitoring error: {str(e)}")

async def cleanup_stale_uploads():
    """Periodically clean up old upload status from memory"""
    while True:
        await asyncio.sleep(300) # Every 5 minutes
        try:
            now = time.time()
            to_remove = []
            
            # Retention periods
            COMPLETED_RETENTION = 3600  # Keep completed/failed for 1 hour (plenty of time for frontend)
            STALLED_RETENTION = 86400   # Keep stalled/abandoned for 24 hours
            
            for upload_id, progress in upload_progress.items():
                status = progress.get('status')
                start_time = progress.get('start_time', now)
                last_update = progress.get('last_update', start_time)
                
                if status in ['completed', 'failed', 'cancelled']:
                    if now - last_update > COMPLETED_RETENTION:
                        to_remove.append(upload_id)
                else:
                    # Stalled uploads
                    if now - last_update > STALLED_RETENTION:
                         to_remove.append(upload_id)
                         
            if to_remove:
                print(f"🧹 CLeaning up {len(to_remove)} stale upload sessions")
                for uid in to_remove:
                    # Cleanup disk if exists
                    upload_dir = os.path.join(CONFIG['UPLOAD_DIR'], uid)
                    if os.path.exists(upload_dir):
                        try:
                            shutil.rmtree(upload_dir)
                        except:
                            pass
                    # Remove from memory
                    del upload_progress[uid]
                    
        except Exception as e:
            print(f"Cleanup error: {str(e)}")

app = FastAPI(lifespan=lifespan)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configuration
CONFIG = {
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://teledrive-hhh9.onrender.com').rstrip('/'),
    'MAX_CHUNK_SIZE': 50 * 1024 * 1024,  # 50MB per chunk
    'UPLOAD_DIR': '/tmp/uploads',
    'BOT_API_SIZE_LIMIT': 20 * 1024 * 1024, # 20MB (Allowed for Bot API)
}

if CONFIG['BACKEND_URL'] == 'http://127.0.0.1:8000':
    print("\n⚠️  WARNING: Using default BACKEND_URL (http://127.0.0.1:8000)")
    print("   If this is a production deployment, ensure BACKEND_URL environment variable is set.\n")
else:
    print(f"\n✅ Worker configured with BACKEND_URL: {CONFIG['BACKEND_URL']}\n")

# In-memory storage for credentials cache and upload progress
credentials_cache = {}
upload_progress = {}
upload_locks = defaultdict(threading.Lock)

# Create upload directory if it doesn't exist
os.makedirs(CONFIG['UPLOAD_DIR'], exist_ok=True)

print(f"Worker started with BACKEND_URL: {CONFIG['BACKEND_URL']}")

# Check for cryptg
try:
    import cryptg
    print("🚀 Cryptg detected! Encryption will be fast.")
except ImportError:
    print("⚠️ Cryptg NOT found. Install it for 10x faster uploads: pip install cryptg")


def read_chunk_sync(file_path, start, size):
    """Sync helper to read file chunk in a thread"""
    with open(file_path, 'rb') as f:
        f.seek(start)
        return f.read(size)


def get_mime_type(filename):
    """Get MIME type from filename"""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type:
        return mime_type
    # Default to octet-stream if unknown
    return 'application/octet-stream'

# ========== LISTENER MANAGER ==========

class ListenerManager:
    def __init__(self):
        self.clients = {} # user_id -> client
        self.is_running = False

    async def start(self):
        """Fetch listeners from backend and start clients"""
        self.is_running = True
        print("Starting Telegram Listeners...")
        
        # Auto-detect worker URL from environment
        # On Render, RENDER_EXTERNAL_URL is automatically set to the public URL
        if 'RENDER_EXTERNAL_URL' in os.environ:
            worker_url = os.environ['RENDER_EXTERNAL_URL']
            print(f"Detected Render deployment: {worker_url}")
        else:
            # Local development fallback
            port = os.environ.get('PORT', '8001')
            worker_url = f"http://127.0.0.1:{port}"
            print(f"Local development mode: {worker_url}") 
        
        try:
            response = requests.get(
                f"{CONFIG['BACKEND_URL']}/api/internal/listeners",
                params={"worker_url": worker_url},
                timeout=10
            )
            
            if response.status_code != 200:
                print(f"Failed to fetch listeners: {response.text}")
                return

            listeners = response.json()
            print(f"Found {len(listeners)} users to listen for")
            
            for user in listeners:
                await self.start_client(user)
                
        except Exception as e:
            print(f"❌ Error starting listeners: Failed to connect to backend at {CONFIG['BACKEND_URL']}")
            print(f"   Details: {str(e)}")
            if "Connection refused" in str(e):
                print("   💡 Hint: The backend might be down or the BACKEND_URL is incorrect.")

    async def start_client(self, user):
        """Start a single user client"""
        try:
            client = TelegramClient(
                StringSession(user['telegram_session']),
                int(os.environ.get('TELEGRAM_API_ID', 2040)), # Fallback for demo
                os.environ.get('TELEGRAM_API_HASH', 'b18441a1ff607e10a989891a5462e627')
            )
            
            await client.connect() 
            
            if not await client.is_user_authorized():
                print(f"User {user['user_id']} session expired")
                return

            # Add event handler
            # We listen to the specific channel ID
            # Note: channel_id from backend might need adjustment if it doesn't match peer
            # But usually we listen to the channel
            
            @client.on(events.NewMessage(chats=[user['telegram_channel_id']]))
            async def handler(event):
                await self.handle_new_message(event, user['user_id'])
                
            self.clients[user['user_id']] = client
            print(f"Listening for user {user['user_id']} on channel {user['telegram_channel_id']}")
            
        except Exception as e:
            print(f"Failed to start client for {user['user_id']}: {str(e)}")

    async def handle_new_message(self, event, user_id):
        """Handle new message event"""
        try:
            if not event.message.file:
                return
            
            # Extract metadata
            file_name = "unknown"
            if event.message.file.name:
                file_name = event.message.file.name
            else:
                # Try to guess extension
                ext = event.message.file.ext or ""
                file_name = f"telegram_{event.message.id}{ext}"
                
            mime_type = event.message.file.mime_type or "application/octet-stream"
            file_size = event.message.file.size
            
            print(f"Detected file: {file_name} ({file_size} bytes) for user {user_id}")
            
            # Register with backend
            payload = {
                "user_id": user_id,
                "name": file_name,
                "size": file_size,
                "mime_type": mime_type,
                "telegram_msg_id": event.message.id,
                "telegram_file_id": None, # Could extract if needed
                "thumbnail_url": None, # Could extract if needed
                "date": event.message.date.isoformat()
            }
            
            requests.post(
                f"{CONFIG['BACKEND_URL']}/api/internal/register-file",
                json=payload,
                timeout=10
            )
            print(f"Registered file {file_name} with backend")
            
        except Exception as e:
            print(f"Error handling message: {str(e)}")

    async def stop(self):
        self.is_running = False
        for user_id, client in self.clients.items():
            await client.disconnect()
        self.clients.clear()

listener_manager = ListenerManager()



def get_credentials(auth_token):
    """Fetch and cache user credentials from backend using Secure RSA Exchange"""
    # Check cache first (cache for 30 seconds)
    cache_key = hashlib.md5(auth_token.encode()).hexdigest()
    if cache_key in credentials_cache:
        cached_data, cached_time = credentials_cache[cache_key]
        if time.time() - cached_time < 30: 
            return cached_data
    
    # Secure Fetch from backend
    try:
        # 1. Generate Ephemeral RSA Key Pair
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=default_backend()
        )
        public_key_pem = private_key.public_key().public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode()
        
        # 2. Send Public Key to Backend
        response = requests.post(
            f"{CONFIG['BACKEND_URL']}/api/worker/credentials",
            json={"public_key": public_key_pem},
            headers={'Authorization': f'Bearer {auth_token}'},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            encrypted_key_b64 = data['encrypted_key']
            encrypted_payload_b64 = data['encrypted_payload']
            
            # 3. Decrypt the Symmetric Key using RSA Private Key
            encrypted_key = base64.b64decode(encrypted_key_b64)
            symmetric_key = private_key.decrypt(
                encrypted_key,
                padding.OAEP(
                    mgf=padding.MGF1(algorithm=hashes.SHA256()),
                    algorithm=hashes.SHA256(),
                    label=None
                )
            )
            
            # 4. Decrypt Payload using Symmetric Key (Fernet)
            f = Fernet(symmetric_key)
            decrypted_payload_json = f.decrypt(encrypted_payload_b64.encode()).decode()
            credentials = json.loads(decrypted_payload_json)
            
            credentials_cache[cache_key] = (credentials, time.time())
            return credentials
            
        else:
            print(f"Failed to fetch credentials: {response.status_code} - {response.text}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching credentials from {CONFIG['BACKEND_URL']}/api/worker/credentials: {str(e)}")
        import traceback
        traceback.print_exc()
        return None



@app.post('/upload-telegram-thumbnail')
async def upload_telegram_thumbnail(
    authToken: str = Form(...),
    file: UploadFile = File(...)
):
    """Upload thumbnail to Telegram channel (always uses Bot API as thumbnails are small)"""
    try:
        # Get credentials
        credentials = get_credentials(authToken)
        if not credentials:
            raise HTTPException(status_code=401, detail='Invalid or expired credentials')
            
        bot_token = credentials.get('bot_token')
        channel_id = credentials.get('channel_id')
        
        if not bot_token or not channel_id:
            raise HTTPException(status_code=400, detail="Bot token or channel ID not configured")

        # Read file content
        content = await file.read()
        
        # Upload to Telegram via Bot API
        files = {'photo': ('thumbnail.jpg', content)}
        data = {'chat_id': channel_id}
        
        # Use requests (sync) or httpx (async) - using requests for simplicity matching existing code
        response = requests.post(
            f'https://api.telegram.org/bot{bot_token}/sendPhoto',
            data=data,
            files=files,
            timeout=30
        )
        
        result = response.json()
        
        if not result.get('ok'):
             raise HTTPException(status_code=500, detail=f"Telegram API error: {result.get('description')}")
             
        # Extract best photo (largest)
        photos = result['result']['photo']
        best_photo = photos[-1]
        
        return {
            'success': True,
            'message_id': result['result']['message_id'],
            'file_id': best_photo['file_id'],
            'file_unique_id': best_photo['file_unique_id']
        }

    except HTTPException:
        raise
    except Exception as e:
        print(f"Thumbnail upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/telegram-thumbnail/{file_id}')
async def get_telegram_thumbnail(file_id: str, auth_token: str = None):
    """Proxy Telegram thumbnail download"""
    try:
        if not auth_token:
            raise HTTPException(status_code=401, detail='Missing auth_token')
            
        # Get credentials
        credentials = get_credentials(auth_token)
        if not credentials:
            raise HTTPException(status_code=401, detail='Invalid or expired credentials')

        bot_token = credentials.get('bot_token')
        
        # 1. Get File Path
        response = requests.get(f'https://api.telegram.org/bot{bot_token}/getFile?file_id={file_id}', timeout=10)
        result = response.json()
        
        if not result.get('ok'):
            raise HTTPException(status_code=404, detail="File not found on Telegram")
            
        file_path = result['result']['file_path']
        
        # 2. Stream File Content
        telegram_url = f'https://api.telegram.org/file/bot{bot_token}/{file_path}'
        
        # Use simple streaming response
        async def iterfile():
            async with httpx.AsyncClient() as client:
                async with client.stream("GET", telegram_url) as r:
                    if r.status_code != 200:
                         return
                    async for chunk in r.aiter_bytes():
                        yield chunk

        # Get content type via HEAD or guess
        content_type = mimetypes.guess_type(file_path)[0] or 'image/jpeg'
        
        return StreamingResponse(
            iterfile(),
            media_type=content_type,
            headers={
                'Cache-Control': 'public, max-age=31536000' # Cache for 1 year
            }
        )

    except HTTPException:
        raise
    except Exception as e:
        print(f"Thumbnail download error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


def upload_to_imagekit(file_obj, private_key, file_name):
    """Upload to ImageKit.io"""
    try:
        url = "https://upload.imagekit.io/api/v1/files/upload"
        files = {'file': file_obj}
        data = {'fileName': file_name, 'useUniqueFileName': 'true'}
        res = requests.post(
            url, 
            files=files, 
            data=data, 
            auth=(private_key, ''), 
            timeout=30
        )
        if res.status_code in [200, 201]:
            return res.json().get('url')
        else:
            print(f"ImageKit Upload Failed: {res.text}")
    except Exception as e:
        print(f"ImageKit error: {e}")
    return None


@app.post('/upload-thumbnail')
async def upload_thumbnail(
    file: UploadFile = File(...),
    authToken: str = Form(...)
):
    """Proxy endpoint to upload thumbnail to ImageKit securely from worker"""
    try:
        # Get credentials
        credentials = get_credentials(authToken)
        if not credentials:
            raise HTTPException(status_code=401, detail='Invalid credentials')
            
        active_provider = credentials.get('active_storage_provider', 'imgbb')
        
        # Only handle ImageKit (others should be client-side)
        if active_provider != 'imagekit':
             raise HTTPException(status_code=400, detail=f'Active provider is {active_provider}, use client-side upload')

        if not credentials.get('imagekit_private_key'):
             raise HTTPException(status_code=500, detail='ImageKit private key missing')

        # Read file
        content = await file.read()
        file_io = io.BytesIO(content)
        
        # Upload
        filename = f"thumb_{int(time.time())}.jpg"
        url = upload_to_imagekit(file_io, credentials['imagekit_private_key'], filename)
        
        if url:
             return {'success': True, 'url': url}
        else:
             raise HTTPException(status_code=500, detail='Upload failed')
             
    except Exception as e:
        print(f"Thumbnail upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/imagekit-auth')
async def imagekit_auth(authToken: str):
    """Generate ImageKit authentication parameters for client-side upload"""
    try:
        credentials = get_credentials(authToken)
        if not credentials:
            raise HTTPException(status_code=401, detail='Invalid credentials')
            
        private_key = credentials.get('imagekit_private_key')
        if not private_key:
            raise HTTPException(status_code=400, detail='ImageKit private key not configured')

        token = str(uuid.uuid4())
        expire = int(time.time()) + 1800  # 30 minutes
        
        # Create signature
        signature_input = (token + str(expire)).encode()
        signature = hmac.new(
            private_key.encode(),
            signature_input,
            hashlib.sha1
        ).hexdigest()

        return {
            "token": token,
            "expire": expire,
            "signature": signature
        }
    except Exception as e:
        print(f"ImageKit auth error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/health')
async def health_check():
    return "ok", 200


@app.get('/bot-file/{file_id}')
async def stream_bot_file(file_id: str, auth_token: str, request_user_id: Optional[str] = None, fileName: Optional[str] = None):
    """Stream Bot API file through worker proxy (always streams to avoid CORS)"""
    try:
        # Get credentials
        credentials = get_credentials(auth_token)
        if not credentials:
            raise HTTPException(status_code=401, detail='Failed to fetch credentials')
        
        bot_token = credentials.get('bot_token')
        
        if not bot_token:
            raise HTTPException(status_code=400, detail='Bot token not configured')
        
        # Call Telegram Bot API to get file path (Async)
        async with httpx.AsyncClient() as client:
            bot_response = await client.get(
                f'https://api.telegram.org/bot{bot_token}/getFile',
                params={'file_id': file_id},
                timeout=10.0
            )
            
            if bot_response.status_code != 200:
                raise HTTPException(status_code=500, detail='Failed to fetch file from Telegram')
            
            bot_data = bot_response.json()
            if not bot_data.get('ok'):
                raise HTTPException(status_code=404, detail='File not found')
            
            file_path = bot_data['result']['file_path']
            download_url = f'https://api.telegram.org/file/bot{bot_token}/{file_path}'
            
            # Use original filename if provided, otherwise fall back to Telegram path
            display_name = fileName or file_path.split("/")[-1]
            
            # HEAD request to get content info
            content_length = None
            content_type = 'application/octet-stream'
            try:
                head_resp = await client.head(download_url)
                if head_resp.status_code == 200:
                    content_length = head_resp.headers.get('content-length')
                    content_type = head_resp.headers.get('content-type', 'application/octet-stream')
            except Exception:
                pass
            
            # Guess content type from filename if Telegram returns generic type
            if fileName and content_type == 'application/octet-stream':
                guessed_type = mimetypes.guess_type(fileName)[0]
                if guessed_type:
                    content_type = guessed_type
            
            headers = {
                'Content-Disposition': f'inline; filename="{display_name}"',
                'Cache-Control': 'public, max-age=3600',
                'Accept-Ranges': 'bytes'
            }
            
            if content_length:
                headers['Content-Length'] = content_length

            # Always stream through worker to avoid CORS issues
            async def iterfile():
                try:
                    async with httpx.AsyncClient(timeout=30.0) as stream_client:
                        async with stream_client.stream("GET", download_url) as response:
                            if response.status_code != 200:
                                return 
                            async for chunk in response.aiter_bytes(chunk_size=1024 * 1024):
                                yield chunk
                except Exception as e:
                    print(f"Stream generator error: {e}")

            return StreamingResponse(
                iterfile(),
                media_type=content_type,
                headers=headers
            )
    
    except HTTPException:
        raise
    except Exception as e:
        print(f"Bot file fetch error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/upload')
async def upload_file(
    authToken: str = Form(...),
    file: UploadFile = File(...)
):
    """Handle legacy single-file upload but with async Telegram upload"""
    try:
        # Get credentials
        credentials = get_credentials(authToken)
        if not credentials:
            raise HTTPException(status_code=401, detail='Failed to fetch credentials')
        
        if not file.filename:
            raise HTTPException(status_code=400, detail='Empty filename')
        
        # Generate upload ID
        upload_id = hashlib.md5(f"{authToken}{file.filename}{time.time()}".encode()).hexdigest()
        # Save with original filename to preserve extension (prefixed with ID for uniqueness)
        file_path = os.path.join(CONFIG['UPLOAD_DIR'], f"{upload_id}_{file.filename}")
        
        # Save file
        with open(file_path, 'wb') as buffer:
            content = await file.read()
            buffer.write(content)
        
        file_size = os.path.getsize(file_path)
        
        # Initialize upload progress
        upload_progress[upload_id] = {
            'status': 'uploading', # Start as uploading immediately
            'file_path': file_path,
            'file_size': file_size,
            'file_name': file.filename,
            'credentials': credentials,
            'telegram_progress': 0,
            'message_id': None,
            'file_id': None,
            'error': None,
            'start_time': time.time(),
            'last_update': time.time()
        }
        
        # Start upload in background thread
        thread = threading.Thread(
            target=upload_to_telegram_background,
            args=(upload_id,)
        )
        thread.daemon = True
        thread.start()
        
        return {
            'success': True,
            'uploadId': upload_id,
            'status': 'uploading',
            'size': file_size
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/upload-chunk')
async def upload_chunk(
    uploadId: str = Form(...),
    chunkIndex: int = Form(...),
    totalChunks: int = Form(...),
    fileName: str = Form(...),
    chunk: UploadFile = File(...),
    authToken: str = Form(...),
    fileSize: int = Form(None)
):
    """Handle chunk upload"""
    try:
        # Validate inputs
        if not uploadId or chunkIndex is None:
            raise HTTPException(status_code=400, detail='Missing required fields')

        # Get credentials (check cache or fetch)
        credentials = get_credentials(authToken)
        if not credentials:
            raise HTTPException(status_code=401, detail='Invalid or expired credentials')

        # Create upload directory for this upload_id if not exists
        upload_dir = os.path.join(CONFIG['UPLOAD_DIR'], uploadId)
        os.makedirs(upload_dir, exist_ok=True)
        
        # Save chunk
        chunk_path = os.path.join(upload_dir, str(chunkIndex))
        content = await chunk.read()
        
        with open(chunk_path, 'wb') as f:
            f.write(content)
            
        # Initialize or update upload progress
        if uploadId not in upload_progress:
            upload_progress[uploadId] = {
                'status': 'uploading_chunks',
                'file_name': fileName,
                'total_chunks': totalChunks,
                'file_size': fileSize,
                'credentials': credentials,
                'received_chunks': set(),
                'telegram_progress': 0,
                'message_id': None,
                'file_id': None,
                'file_id': None,
                'error': None,
                'start_time': time.time(),
                'last_update': time.time()
            }
        
        # Track received chunk
        if 'received_chunks' not in upload_progress[uploadId]:
             upload_progress[uploadId]['received_chunks'] = set()
             
        upload_progress[uploadId]['received_chunks'].add(chunkIndex)
        upload_progress[uploadId]['last_update'] = time.time()
        
        return {
            "success": True, 
            "chunkIndex": chunkIndex,
            "receivedChunks": len(upload_progress[uploadId]['received_chunks'])
        }

    except Exception as e:
        print(f"Chunk upload error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/upload-status/{upload_id}')
async def get_chunk_upload_status(upload_id: str):
    """Get status of chunked upload for resume capability"""
    if upload_id not in upload_progress:
        print(f"⚠️  Upload status requested for unknown ID: {upload_id}")
        print("   💡 Possible cause: Worker restarted and cleared in-memory progress.")
        raise HTTPException(status_code=404, detail='Upload not found or worker restarted')
        
    progress = upload_progress[upload_id]
    
    # Check if it was a completed upload
    if progress.get('status') == 'completed':
         return {
            "status": "completed",
            "uploadedChunks": list(range(progress.get('total_chunks', 0))),
            "messageId": progress.get('message_id'),
            "fileId": progress.get('file_id')
        }
        
    # Return list of received chunks
    received_chunks = list(progress.get('received_chunks', []))
    return {
        "status": progress.get('status', 'unknown'),
        "uploadedChunks": received_chunks,
        "uploadId": upload_id
    }


@app.post('/cancel-upload')
async def cancel_upload(data: dict):
    """Cancel upload and clean up chunks"""
    upload_id = data.get('uploadId')
    if not upload_id:
        raise HTTPException(status_code=400, detail='Missing uploadId')
        
    try:
        # Remove from progress tracking
        if upload_id in upload_progress:
            del upload_progress[upload_id]
            
        # Remove chunks directory
        upload_dir = os.path.join(CONFIG['UPLOAD_DIR'], upload_id)
        if os.path.exists(upload_dir):
            shutil.rmtree(upload_dir)
            
        return {"success": True}
    except Exception as e:
        print(f"Cancel error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/complete-upload')
async def complete_upload(request: Request):
    """Complete the upload by assembling chunks and sending to Telegram"""
    try:
        data = await request.json()
        upload_id = data.get('uploadId')
        
        if not upload_id or upload_id not in upload_progress:
            print(f"Complete upload failed: Invalid upload ID {upload_id} (Available: {list(upload_progress.keys())})")
            raise HTTPException(status_code=400, detail='Invalid upload ID')
        
        progress = upload_progress[upload_id]
        
        # If it's a legacy single-file upload
        if progress.get('status') == 'uploaded':
             # Already ready to upload to telegram
             pass
        # If it's a chunked upload
        elif progress.get('status') == 'uploading_chunks':
            # Verify we have all chunks
            total_chunks = progress.get('total_chunks')
            received_chunks = progress.get('received_chunks', set())
            
            if len(received_chunks) < total_chunks:
                print(f"Complete upload failed: Incomplete chunks for {upload_id} ({len(received_chunks)}/{total_chunks})")
                raise HTTPException(status_code=400, detail=f"Incomplete upload: {len(received_chunks)}/{total_chunks} chunks")
            
            # Assemble file
            upload_dir = os.path.join(CONFIG['UPLOAD_DIR'], upload_id)
            # Use original filename for the assembled file (prefixed with ID)
            file_name = progress.get('file_name', 'merged_file')
            final_file_path = os.path.join(CONFIG['UPLOAD_DIR'], f"{upload_id}_{file_name}")
            
            print(f"Assembling {total_chunks} chunks for {upload_id}...")
            
            with open(final_file_path, 'wb') as outfile:
                for i in range(total_chunks):
                    chunk_path = os.path.join(upload_dir, str(i))
                    if not os.path.exists(chunk_path):
                        raise HTTPException(status_code=400, detail=f"Missing chunk {i}")
                        
                    with open(chunk_path, 'rb') as infile:
                        outfile.write(infile.read())
            
            # Cleanup chunks directory
            try:
                shutil.rmtree(upload_dir)
            except:
                pass
                
            # Update progress object for telegram upload phase
            progress['status'] = 'assembled'
            progress['file_path'] = final_file_path
            progress['file_size'] = os.path.getsize(final_file_path)
            # Remove set as it's not JSON serializable if we dump it later, though we keep in memory
            if 'received_chunks' in progress:
                del progress['received_chunks']
                
        elif progress.get('status') == 'completed':
            return {
                'status': 'completed',
                'messageId': progress['message_id'],
                'fileId': progress['file_id']
            }
        elif progress.get('status') == 'uploading':
             print(f"DEBUG: Complete upload called while uploading (idempotent success) for {upload_id}")
             # Return idempotent success
             return {
                 'status': 'uploading',
                 'uploadId': upload_id,
                 'message': 'Upload to Telegram already in progress'
             }
        
        # Start upload in background thread
        progress['status'] = 'uploading'
        progress['telegram_progress'] = 0
        
        thread = threading.Thread(
            target=upload_to_telegram_background,
            args=(upload_id,)
        )
        thread.daemon = True
        thread.start()
        
        # Return immediately
        return {
            'status': 'uploading',
            'uploadId': upload_id,
            'message': 'Upload to Telegram started in background'
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Complete upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


def upload_to_telegram_background(upload_id):
    """Background function to upload file to Telegram"""
    try:
        progress = upload_progress[upload_id]
        file_path = progress['file_path']
        file_size = progress['file_size']
        credentials = progress['credentials']
        
        # Decide whether to use Bot API or Client API
        if file_size <= CONFIG['BOT_API_SIZE_LIMIT']:
            # Use Bot API for files <= 50MB
            upload_with_bot_api(upload_id, file_path, credentials)
        else:
            # Use Telethon Client API for files > 50MB
            upload_with_client_api(upload_id, file_path, credentials)
            
    except Exception as e:
        print(f"Background upload error for {upload_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        upload_progress[upload_id]['status'] = 'failed'
        upload_progress[upload_id]['error'] = str(e)
    finally:
        # Cleanup file after upload (success or failure)
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            print(f"Error cleaning up file: {str(e)}")


def upload_with_bot_api(upload_id, file_path, credentials):
    """Upload file using Telegram Bot API (files <= 50MB)"""
    try:
        progress = upload_progress[upload_id]
        bot_token = credentials.get('bot_token')
        channel_id = credentials.get('channel_id')
        file_name = progress['file_name']
        
        if not bot_token or not channel_id:
            raise Exception("Bot token or channel ID not configured")
        
        print(f"Uploading {file_name} via Bot API...")
        
        # Upload file to Telegram
        with open(file_path, 'rb') as f:
            files = {'document': (file_name, f)}
            data = {'chat_id': channel_id}
            
            response = requests.post(
                f'https://api.telegram.org/bot{bot_token}/sendDocument',
                data=data,
                files=files,
                timeout=300  # 5 minutes timeout
            )
        
        result = response.json()
        
        if not result.get('ok'):
            raise Exception(f"Telegram API error: {result.get('description', 'Unknown error')}")
        
        # Extract file_id from response
        telegram_result = result['result']
        file_id = (
            telegram_result.get('document', {}).get('file_id') or
            telegram_result.get('video', {}).get('file_id') or
            telegram_result.get('audio', {}).get('file_id') or
            (telegram_result.get('photo', [{}])[0].get('file_id') if telegram_result.get('photo') else None)
        )
        
        if not file_id:
            raise Exception('Failed to get file_id from Telegram response')
        
        # Update progress
        progress['status'] = 'completed'
        progress['telegram_progress'] = 100
        progress['message_id'] = telegram_result['message_id']
        progress['file_id'] = file_id
        progress['last_update'] = time.time()
        
        print(f"Bot API upload completed: message_id={telegram_result['message_id']}, file_id={file_id}")
        print(f"✅ Processing complete for {file_name}")
        
    except Exception as e:
        print(f"Bot API upload error: {str(e)}")
        raise


def upload_with_client_api(upload_id, file_path, credentials):
    """Upload large file using Telethon Client API (files > 50MB)"""
    try:
        progress = upload_progress[upload_id]
        file_name = progress['file_name']
        
        # Validate credentials
        required_fields = ['telegram_session', 'telegram_api_id', 'telegram_api_hash', 'channel_id']
        missing_fields = [field for field in required_fields if not credentials.get(field)]
        
        if missing_fields:
            raise Exception(f"Missing required credentials: {', '.join(missing_fields)}")
        
        print(f"Uploading {file_name} via Telethon Client API...")
        print(f"Credentials check: session={'present' if credentials.get('telegram_session') else 'missing'}, "
              f"api_id={credentials.get('telegram_api_id')}, channel_id={credentials.get('channel_id')}")
        
        # Run async upload in new event loop
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            result = loop.run_until_complete(
                upload_to_telegram_client(
                    file_path,
                    file_name,
                    credentials,
                    upload_id
                )
            )
            
            # Update progress with result
            progress['status'] = 'completed'
            progress['telegram_progress'] = 100
            progress['file_id'] = result['file_id']
            progress['message_id'] = result['message_id']
            progress['last_update'] = time.time()
            
            print(f"Telethon upload completed: message_id={result['message_id']}, file_id={result['file_id']}")
            print(f"✅ Processing complete for {file_name}")
            
        finally:
            loop.close()
            
    except Exception as e:
        print(f"Client API upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


async def fast_upload(client, file_path, file_size, progress_callback=None, task_id=None):
    """Upload file in parallel chunks using Telethon"""
    # 512KB is the max part size for SaveBigFilePartRequest
    part_size = 512 * 1024
    part_count = (file_size + part_size - 1) // part_size
    # Generate random ID safely
    import random
    file_id = random.randint(1000000000000, 9999999999999)
    
    # Semaphore to limit concurrency (15 workers for better speed on Render)
    sem = asyncio.Semaphore(15)
    
    uploaded_bytes = 0
    lock = asyncio.Lock()
    
    async def upload_part(part_index):
        nonlocal uploaded_bytes
        
        async with sem:
            # Check for cancellation
            if task_id and (task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled'):
                print(f"Fast upload cancelled for {task_id}")
                return # Stop this chunk
            
            start = part_index * part_size
            
            # Read chunk in thread pool to avoid blocking async loop
            loop = asyncio.get_running_loop()
            chunk = await loop.run_in_executor(
                io_pool, 
                read_chunk_sync, 
                file_path, 
                start, 
                part_size
            )
            
            if not chunk:
                return
                
            # Upload chunk
            await client(SaveBigFilePartRequest(
                file_id=file_id,
                file_part=part_index,
                file_total_parts=part_count,
                bytes=chunk
            ))
            
            # Update progress
            if progress_callback:
                async with lock:
                    uploaded_bytes += len(chunk)
                    progress_callback(uploaded_bytes, file_size)

    # Create tasks for all parts
    tasks = [upload_part(i) for i in range(part_count)]
    
    # Wait for all parts to upload
    await asyncio.gather(*tasks)
    
    return InputFileBig(
        id=file_id,
        parts=part_count,
        name=os.path.basename(file_path)
    )


async def upload_to_telegram_client(file_path, file_name, credentials, upload_id):
    """Upload file to Telegram using Telethon with progress tracking"""
    client = None
    try:
        # Initialize Telethon client with optimized settings
        client = TelegramClient(
            StringSession(credentials['telegram_session']),
            int(credentials['telegram_api_id']),
            credentials['telegram_api_hash'],
            connection_retries=5,
            retry_delay=1,
            flood_sleep_threshold=60  # Wait up to 60s for flood errors
        )
        
        await client.connect()
        print("Telethon client connected (optimized settings)")
        
        # Get channel entity
        channel_id = int(credentials['channel_id'])
        channel = await client.get_entity(channel_id)
        print(f"Channel entity resolved: {channel.id}")
        
        # Progress callback
        last_logged_percent = -1
        
        def progress_callback(current, total):
            nonlocal last_logged_percent
            progress_percent = int((current / total) * 100)
            upload_progress[upload_id]['telegram_progress'] = progress_percent
            
            # Log only when percentage changes and hits a 5% marker, or strictly every 10%
            if progress_percent != last_logged_percent:
                if progress_percent % 10 == 0:
                    print(f"Upload progress: {progress_percent}% ({current}/{total} bytes)")
                    last_logged_percent = progress_percent
        
        # Upload file
        print(f"Starting Telethon parallel upload: {file_name}")
        file_size = os.path.getsize(file_path)
        
        # Use fast_upload for parallel uploading
        input_file = await fast_upload(client, file_path, file_size, progress_callback, task_id=upload_id)
        
        # Post-upload check
        if upload_id in import_cancellation_flags or upload_progress.get(upload_id, {}).get('status') == 'cancelled':
            raise Exception("Cancelled by user")
        
        # Determine if we should force document type (e.g. for large images > 10MB)
        force_document = False
        if file_size > 10 * 1024 * 1024: # 10MB
            force_document = True
            print(f"File > 10MB ({file_size}), forcing document upload")

        message = await client.send_file(
            channel,
            input_file, # Pass the uploaded file handle
            caption=file_name,
            force_document=force_document
            # Progress callback is already handled in fast_upload, but send_file might use it 
            # for the final "send", though usually instant. 
            # We don't pass it again to avoid double counting or confusion, 
            # as fast_upload handles the bulk of the work.
        )
        
        print(f"Telethon upload successful: message_id={message.id}")
        
        # Extract file_id from message
        file_id = None
        if message.document:
            file_id = message.document.id
        elif message.video:
            file_id = message.video.id
        elif message.audio:
            file_id = message.audio.id
        elif message.photo:
            file_id = message.photo.id
        
        return {
            'message_id': message.id,
            'file_id': str(file_id) if file_id else None
        }
        
    finally:
        if client:
            await client.disconnect()
            print("Telethon client disconnected")


@app.get('/upload-progress/{upload_id}')
async def get_upload_progress(upload_id: str):
    """Get upload progress for a specific upload ID"""
    try:
        if upload_id not in upload_progress:
            raise HTTPException(status_code=404, detail='Upload ID not found')
        
        progress = upload_progress[upload_id]
        
        return {
            'status': progress.get('status'),
            'progress': progress.get('telegram_progress', 0),
            'telegram_progress': progress.get('telegram_progress', 0),
            'messageId': progress.get('message_id'),
            'fileId': progress.get('file_id'),
            'error': progress.get('error')
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Progress check error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get('/download')
async def download_file(request: Request, messageId: str, token: str, fileName: str = 'file'):
    """Download files from Telegram with Range request support for chunked downloads"""
    try:
        if not messageId or not token:
            raise HTTPException(status_code=400, detail='Missing messageId or token')
        
        # Verify token with backend
        try:
            verify_response = requests.post(
                f"{CONFIG['BACKEND_URL']}/api/worker/verify-download-token",
                data={'token': token},
                timeout=10
            )
            
            if verify_response.status_code != 200:
                raise HTTPException(status_code=401, detail='Invalid or expired token')
            
            credentials = verify_response.json()
        except HTTPException:
            raise
        except Exception as e:
            print(f"Token verification failed: {str(e)}")
            raise HTTPException(status_code=401, detail='Failed to verify token')
        
        # Get Range header if present
        range_header = request.headers.get('Range')
        
        if range_header:
            # Parse range header: "bytes=0-5242879"
            try:
                range_str = range_header.replace('bytes=', '')
                if '-' in range_str:
                    parts = range_str.split('-')
                    range_start = int(parts[0]) if parts[0] else 0
                    range_end = int(parts[1]) if parts[1] else None
                else:
                    range_start = 0
                    range_end = None
            except Exception as e:
                print(f"Error parsing Range header '{range_header}': {e}")
                raise HTTPException(status_code=416, detail='Invalid Range header')
            
            print(f"Range request: {range_start}-{range_end}")
            
            # Stream specific byte range from Telegram
            return await stream_file_range(
                request,
                messageId, 
                credentials, 
                fileName, 
                range_start, 
                range_end
            )
        else:
            # Full file download (for small files or legacy support)
            print(f"Full file download request: {fileName}")
            return await stream_full_file(request, messageId, credentials, fileName)
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Download error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


async def stream_file_range(request: Request, message_id, credentials, file_name, range_start, range_end):
    """Stream a specific byte range from Telegram file using pure async generator"""
    
    
    # Try to reuse existing client from listener to save connection time
    user_id = credentials.get('user_id')
    cached_client = listener_manager.clients.get(user_id) if user_id else None
    
    should_disconnect = True
    client = None
    
    if cached_client and cached_client.is_connected():
        print(f"♻️ Reusing active client for user {user_id}")
        client = cached_client
        should_disconnect = False
    else:
        # Create new client if no cached one available
        print(f"Creating new client for download (User {user_id} not active listener)")
        client = TelegramClient(
            StringSession(credentials['telegram_session']),
            int(credentials['telegram_api_id']),
            credentials['telegram_api_hash']
        )
        await client.connect()
    
    try:
        if not client.is_connected():
            await client.connect()
            
        channel = await client.get_entity(int(credentials['channel_id']))
        message = await client.get_messages(channel, ids=int(message_id))
        
        if not message or not message.file:
            raise Exception(f"Message {message_id} not found or has no file")
        
        file_size = message.file.size
        
        # Adjust range_end if not specified or exceeds file size
        actual_end = min(range_end if range_end is not None else file_size - 1, file_size - 1)
        bytes_to_send = actual_end - range_start + 1
        
        print(f"Streaming range {range_start}-{actual_end} ({bytes_to_send} bytes) from file size {file_size}")
        
        # Create async generator for buffered streaming
        async def generate_chunks():
            """Async generator that yields chunks from Telegram with look-ahead buffering"""
            # Buffer configuration
            CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
            BUFFER_SIZE = 8               # 32MB Buffer
            
            queue = asyncio.Queue(maxsize=BUFFER_SIZE)
            sentinel = object()
            
            # Producer: Downloads from Telegram and puts into queue
            async def producer():
                try:
                    downloaded = 0
                    async for chunk in client.iter_download(
                        message.media,
                        offset=range_start,
                        limit=bytes_to_send,
                        chunk_size=CHUNK_SIZE
                    ):
                        if await request.is_disconnected():
                            print(f"Client disconnected (producer detected), stopping...")
                            break
                            
                        await queue.put(chunk)
                        
                        downloaded += len(chunk)
                        if downloaded % (5 * 1024 * 1024) == 0:
                            print(f"Buffered: {downloaded}/{bytes_to_send} bytes")
                            
                    await queue.put(sentinel)
                except Exception as e:
                    print(f"Producer error: {e}")
                    # Signal error to consumer
                    await queue.put(e)
            
            # Start producer task
            producer_task = asyncio.create_task(producer())
            
            # Consumer: Yields from queue to response
            try:
                while True:
                    # Check for disconnect
                    if await request.is_disconnected():
                         print("Client disconnected (consumer detected)")
                         break

                    # Get chunk from buffer
                    item = await queue.get()
                    
                    if item is sentinel:
                        break
                    
                    if isinstance(item, Exception):
                        raise item
                        
                    yield bytes(item)
                    queue.task_done()
                    
            except Exception as e:
                print(f"Streaming error: {str(e)}")
                # Cancel producer if consumer fails
                producer_task.cancel()
                raise
            finally:
                # Ensure we wait for producer to clean up if needed
                try:
                    await producer_task
                except:
                    pass
                # Only disconnect if we created the client locally
                if should_disconnect and client:
                    await client.disconnect()
        
        # Detect MIME type from filename
        mime_type = get_mime_type(file_name)
        
        # Return partial content response (206) with proper headers
        return StreamingResponse(
            generate_chunks(),
            status_code=206,
            media_type=mime_type,
            headers={
                'Content-Disposition': f'inline; filename="{file_name}"',  # inline for browser playback
                'Accept-Ranges': 'bytes',
                'Content-Range': f'bytes {range_start}-{actual_end}/{file_size}',
                'Content-Length': str(bytes_to_send),
                'Cache-Control': 'public, max-age=3600, immutable',
                'X-Accel-Buffering': 'no'
            }
        )
    except Exception as e:
        # Ensure we disconnect on error if we own the client
        if should_disconnect and client:
            await client.disconnect()
        raise


async def stream_full_file(request: Request, message_id, credentials, file_name):
    """Stream entire file using pure async generator"""
    
    
    # Try to reuse existing client from listener to save connection time
    user_id = credentials.get('user_id')
    cached_client = listener_manager.clients.get(user_id) if user_id else None
    
    should_disconnect = True
    client = None
    
    if cached_client and cached_client.is_connected():
        print(f"♻️ Reusing active client for user {user_id}")
        client = cached_client
        should_disconnect = False
    else:
        # Create new client if no cached one available
        print(f"Creating new client for download (User {user_id} not active listener)")
        client = TelegramClient(
            StringSession(credentials['telegram_session']),
            int(credentials['telegram_api_id']),
            credentials['telegram_api_hash']
        )
        await client.connect()
    
    try:
        if not client.is_connected():
            await client.connect()
            
        channel = await client.get_entity(int(credentials['channel_id']))
        message = await client.get_messages(channel, ids=int(message_id))
        
        if not message or not message.file:
            raise Exception(f"Message {message_id} not found or has no file")
        
        file_size = message.file.size
        
        async def generate_chunks():
            """Async generator for full file download with buffering"""
            # Buffer configuration
            CHUNK_SIZE = 4 * 1024 * 1024  # 4MB
            BUFFER_SIZE = 8               # 32MB Buffer
            
            queue = asyncio.Queue(maxsize=BUFFER_SIZE)
            sentinel = object()
            
            async def producer():
                try:
                    downloaded = 0
                    async for chunk in client.iter_download(message.media, chunk_size=CHUNK_SIZE):
                        if await request.is_disconnected():
                            print(f"Client disconnected during download, stopping...")
                            break
                        
                        await queue.put(chunk)
                        downloaded += len(chunk)
                        
                        if downloaded % (10 * 1024 * 1024) == 0:
                            print(f"Buffered: {downloaded}/{message.file.size} bytes")
                            
                    await queue.put(sentinel)
                except Exception as e:
                    print(f"Producer error: {e}")
                    await queue.put(e)

            producer_task = asyncio.create_task(producer())
            
            while True:
                if await request.is_disconnected():
                     break
                
                item = await queue.get()
                if item is sentinel:
                    break
                
                if isinstance(item, Exception):
                    raise item
                
                yield bytes(item)
                queue.task_done()
                
            print(f"Full download complete")
                
        # Detect MIME type from filename
        mime_type = get_mime_type(file_name)
        
        return StreamingResponse(
            generate_chunks(),
            media_type=mime_type,
            headers={
                'Content-Disposition': f'attachment; filename="{file_name}"',  # attachment forces download
                'Accept-Ranges': 'bytes',
                'Content-Length': str(file_size),
                'Cache-Control': 'public, max-age=3600, immutable',
                'X-Accel-Buffering': 'no'
            }
        )
            
    except Exception as e:
        print(f"Streaming error: {str(e)}")
        import traceback
        traceback.print_exc()
        if should_disconnect and client:
            await client.disconnect()
        raise




# ========== LINK IMPORT FEATURE ==========

class ImportRequest(BaseModel):
    url: str
    user_id: str
    target_folder_id: Optional[str] = None
    telegram_auth: dict
    task_id: str

@app.post('/import-url')
async def import_from_url_endpoint(req: ImportRequest, background_tasks: BackgroundTasks):
    """Start background import task"""
    task_id = req.task_id
    import_progress[task_id] = {
        'status': 'pending',
        'progress': 0,
        'download_progress': 0,
        'upload_progress': 0,
        'phase': 'Queued'
    }
    
    background_tasks.add_task(process_import_job, req)
    
    return {"status": "accepted", "task_id": task_id}

class CancelImportRequest(BaseModel):
    task_id: str

@app.post('/cancel-import')
async def cancel_import_endpoint(req: CancelImportRequest):
    """Cancel a running import task"""
    print(f"DEBUG: Received Cancel Request for {req.task_id}. Current Keys: {list(import_progress.keys())}")
    
    if req.task_id in import_progress:
        import_cancellation_flags.add(req.task_id)
        import_progress[req.task_id]['status'] = 'cancelled'
        import_progress[req.task_id]['phase'] = 'Cancelled by user'
        
        # Kill subprocess if exists (for GDrive)
        if req.task_id in active_subprocesses:
            try:
                print(f"Killing subprocess for {req.task_id}")
                active_subprocesses[req.task_id].terminate()
            except Exception as e:
                print(f"Error killing subprocess: {e}")

        print(f"DEBUG: Cancelled {req.task_id}. Flags: {import_cancellation_flags}")
        return {"status": "cancelled"}
    
    print(f"DEBUG: Task {req.task_id} NOT FOUND in import_progress")
    return {"status": "not_found"}

@app.get('/import-progress/{task_id}')
async def get_import_progress(task_id: str):
    if task_id not in import_progress:
        raise HTTPException(status_code=404, detail="Task not found")
    return import_progress[task_id]

def download_gdown_worker(task_id, temp_dir, url, is_folder):
    """Sync worker for GDrive downloads using Subprocess for cancellability"""
    downloaded_files = []
    
    # Check cancel before starting
    if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
        print(f"🛑 GDown worker check 1: Cancellation detected for {task_id}")
        raise Exception("Cancelled by user")

    # Use sys.executable -m gdown to run as CLI
    cmd = [sys.executable, '-m', 'gdown', url, '-O', temp_dir, '--no-cookies']
    if is_folder:
        import_progress[task_id]['phase'] = 'Downloading GDrive Folder...'
        cmd.append('--folder')
    else:
        import_progress[task_id]['phase'] = 'Downloading GDrive File...'
        # For files, gdown auto-detects name if targeting a dir, usually.
        # But to be safe, we just output to temp_dir.
        # gdown CLI with -O as directory works for --folder, but for file it might expect a filename?
        # gdown docs: -O output. If it's a dir, it saves inside.
        # Let's verify behavior. Usually -O handles dir if exists.
        # We'll use --fuzzy to match lib behavior
        cmd.append('--fuzzy')

    if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
        print(f"🛑 GDown worker check 2: Cancellation detected for {task_id}")
        raise Exception("Cancelled by user")

    print(f"Starting GDown: {cmd}")
    try:
        # Start Process
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        active_subprocesses[task_id] = proc
        
        stdout, stderr = proc.communicate()
        
        if proc.returncode != 0:
            # Check if it was because of our termination
            if task_id in import_cancellation_flags:
                raise Exception("Cancelled by user")
            # Otherwise error
            raise Exception(f"GDown failed: {stderr.decode('utf-8', errors='ignore')}")

    finally:
        # cleanup dict
        active_subprocesses.pop(task_id, None)

    if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
        raise Exception("Cancelled by user")
    
    # Walk dir to find files
    for root, _, filenames in os.walk(temp_dir):
        for name in filenames:
            downloaded_files.append(os.path.join(root, name))
            
    return downloaded_files

def download_direct_worker(task_id, temp_dir, url):
    """Sync worker for Direct Link downloads"""
    downloaded_files = []
    
    if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
        print(f"🛑 Direct worker check 1: Cancellation detected for {task_id}")
        raise Exception("Cancelled by user")
    
    import_progress[task_id]['phase'] = 'Downloading File...'
    
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        total_length = int(r.headers.get('content-length', 0))
        
        # Determine Filename
        filename = None
        if "content-disposition" in r.headers:
            cd = r.headers["content-disposition"]
            fname_match = re.findall(r'filename="?([^"]+)"?', cd)
            if fname_match: filename = fname_match[0]
        
        if not filename: filename = url.split('/')[-1].split('?')[0]
        if not filename or len(filename) < 2: filename = f"file_{uuid.uuid4().hex[:8]}"
            
        if '.' not in filename:
            ext = mimetypes.guess_extension(r.headers.get('content-type', ''))
            if ext: filename += ext

        filepath = os.path.join(temp_dir, filename)
        
        dl = 0
        debug_counter = 0
        with open(filepath, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                debug_counter += 1
                if debug_counter % 1000 == 0:
                     cancelled_status = import_progress.get(task_id, {}).get('status')
                     print(f"DEBUG: DirectLoop {task_id}. Flag: {task_id in import_cancellation_flags}, Status: {cancelled_status}")

                if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
                    print(f"🛑 Direct download loop cancelled for {task_id}")
                    raise Exception("Cancelled by user")
                dl += len(chunk)
                f.write(chunk)
                if total_length:
                     prog = int((dl / total_length) * 100)
                     import_progress[task_id]['download_progress'] = prog
                     import_progress[task_id]['progress'] = int(prog / 2)
    
    if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
        raise Exception("Cancelled by user")
    downloaded_files.append(filepath)
    return downloaded_files

async def process_import_job(req: ImportRequest):
    task_id = req.task_id
    temp_dir = f"temp_import_{task_id}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled':
            raise Exception("Cancelled by user")

        import_progress[task_id].update({
            'status': 'downloading',
            'progress': 0,
            'download_progress': 0,
            'upload_progress': 0,
            'phase': 'Downloading...'
        })
        
        downloaded_files = [] # List of (path, filename)
        
        
        # 1. Determine Source & Download IO offload
        loop = asyncio.get_running_loop()
        
        if 'drive.google.com' in req.url:
            # GDrive
            is_folder = '/folders/' in req.url
            downloaded_files = await loop.run_in_executor(
                io_pool, 
                download_gdown_worker, 
                task_id, 
                temp_dir, 
                req.url, 
                is_folder
            )
        else:
            # Direct URL
            downloaded_files = await loop.run_in_executor(
                io_pool, 
                download_direct_worker, 
                task_id, 
                temp_dir, 
                req.url
            )

        if not downloaded_files:
             raise Exception("Download failed")
        
        # Ensure download progress is 100% before moving next
        import_progress[task_id]['download_progress'] = 100
        import_progress[task_id]['phase'] = 'Connecting to Telegram...'
        
        # 2. Upload to Telegram
        total_files = len(downloaded_files)
        
        # Initialize granular file tracking
        files_metadata = []
        for fpath in downloaded_files:
            files_metadata.append({
                'name': os.path.basename(fpath),
                'size': os.path.getsize(fpath),
                'status': 'pending',
                'progress': 0
            })
        import_progress[task_id]['files'] = files_metadata
        
        # Determine if we can use Bot API
        bot_token = req.telegram_auth.get('bot_token')
        channel_id = req.telegram_auth['channel_id']
        
        # Telethon Client (lazy init)
        client = None
        
        
        
        for idx, file_path in enumerate(downloaded_files):
            if task_id in import_cancellation_flags or import_progress.get(task_id, {}).get('status') == 'cancelled': 
                raise Exception("Cancelled by user")
            
            fname = os.path.basename(file_path)
            fsize = os.path.getsize(file_path)
            import_progress[task_id]['phase'] = f"Uploading {idx+1}/{total_files}: {fname}"
            
            # Update specific file status
            if 'files' in import_progress[task_id] and idx < len(import_progress[task_id]['files']):
                import_progress[task_id]['files'][idx]['status'] = 'uploading'
            
            uploaded_msg_id = None
            uploaded_file_id = None
            
            # Try Bot API
            if bot_token and fsize <= CONFIG['BOT_API_SIZE_LIMIT']:
                import_progress[task_id]['phase'] += " (Bot API)"
                print(f"Uploading {fname} ({fsize} bytes) via Bot API")
                
                try:
                    with open(file_path, 'rb') as f:
                        bot_res = requests.post(
                            f"https://api.telegram.org/bot{bot_token}/sendDocument",
                            data={"chat_id": channel_id, "caption": fname},
                            files={"document": (fname, f)},
                            timeout=300 # 5 min timeout for 50MB
                        )
                    
                    if bot_res.status_code == 200:
                        res_json = bot_res.json()
                        if res_json.get('ok'):
                            msg = res_json['result']
                            uploaded_msg_id = str(msg['message_id'])
                            # Extract file_id
                            if 'document' in msg:
                                uploaded_file_id = msg['document']['file_id']
                            elif 'photo' in msg:
                                uploaded_file_id = msg['photo'][-1]['file_id']
                            elif 'video' in msg:
                                uploaded_file_id = msg['video']['file_id']
                            elif 'audio' in msg:
                                uploaded_file_id = msg['audio']['file_id']
                            
                            import_progress[task_id]['upload_progress'] = int(((idx + 1) / total_files) * 100)
                            import_progress[task_id]['progress'] = int(50 + ((idx + 1) / total_files * 50))
                            
                            # Update file progress
                            if 'files' in import_progress[task_id] and idx < len(import_progress[task_id]['files']):
                                import_progress[task_id]['files'][idx]['progress'] = 100
                except Exception as ex:
                    print(f"Bot API upload failed, falling back to Client: {ex}")

            # Fallback to Client API
            if not uploaded_msg_id:
                if not client:
                    import_progress[task_id]['phase'] = 'Connecting to Telegram Client...'
                    client = TelegramClient(
                        StringSession(req.telegram_auth['session']),
                        int(req.telegram_auth['api_id']),
                        req.telegram_auth['api_hash'],
                        connection_retries=5,
                        retry_delay=1,
                        flood_sleep_threshold=60
                    )
                    await client.connect()
                    target_channel = await client.get_entity(int(channel_id))

                # Progress callback wrapper
                def callback(current, total):
                    file_share = 100 / total_files
                    base_progress = idx * file_share
                    current_file_progress = (current / total) * file_share
                    
                    # Update upload specific progress
                    total_upload_prog = int(base_progress + current_file_progress)
                    import_progress[task_id]['upload_progress'] = total_upload_prog
                    
                    # Update global progress (50-100 mapped)
                    import_progress[task_id]['progress'] = int(50 + (total_upload_prog / 2))
                    
                    # Update file progress
                    if 'files' in import_progress[task_id] and idx < len(import_progress[task_id]['files']):
                        import_progress[task_id]['files'][idx]['progress'] = int((current / total) * 100)

                # Use fast_upload for large files (> 10MB) for better speed
                if fsize > 10 * 1024 * 1024:
                    import_progress[task_id]['phase'] = f"Uploading {idx+1}/{total_files}: {fname} (Fast Mode)"
                    print(f"Using fast_upload for {fname} ({fsize} bytes)")
                    
                    # Upload file in parallel chunks
                    input_file = await fast_upload(client, file_path, fsize, callback, task_id=task_id)
                    
                    # Send the uploaded file
                    uploaded_msg = await client.send_file(
                        target_channel,
                        input_file,
                        caption=fname
                    )
                else:
                    # Use regular send_file for smaller files
                    uploaded_msg = await client.send_file(
                        target_channel,
                        file_path,
                        caption=fname,
                        progress_callback=callback
                    )
                
                uploaded_msg_id = str(uploaded_msg.id)
                uploaded_file_id = get_file_id_fast(uploaded_msg)
            
            # 3. Register with Backend (Webhook)
            file_meta = {
                'name': fname,
                'size': fsize,
                'mime_type': mimetypes.guess_type(file_path)[0] or 'application/octet-stream',
                'telegram_msg_id': uploaded_msg_id,
                'telegram_file_id': uploaded_file_id,
                'folder_id': req.target_folder_id,
                'user_id': req.user_id,
                'process_faces': False,
                'needs_processing': mimetypes.guess_type(file_path)[0].startswith('image/') if mimetypes.guess_type(file_path)[0] else False
            }
            
            try:
                web_res = requests.post(
                    f"{CONFIG['BACKEND_URL']}/api/webhook/upload",
                    json=file_meta,
                    timeout=10
                )
                if web_res.status_code != 200:
                    print(f"Webhook Failed: {web_res.status_code} - {web_res.text}")
                else:
                    reg_data = web_res.json()
                    print(f"File registered: {fname} (ID: {reg_data.get('file_id')})")
                    # Store file_id in files tracking
                    if 'files' in import_progress[task_id] and idx < len(import_progress[task_id]['files']):
                        import_progress[task_id]['files'][idx]['file_id'] = reg_data.get('file_id')
            except Exception as we:
                print(f"Webhook Connection Error: {we}")

            # Mark file as completed
            if 'files' in import_progress[task_id] and idx < len(import_progress[task_id]['files']):
                import_progress[task_id]['files'][idx]['status'] = 'completed'
                import_progress[task_id]['files'][idx]['progress'] = 100
        
        if client:
            await client.disconnect()
            
        import_progress[task_id].update({
            'status': 'completed', 
            'progress': 100, 
            'download_progress': 100,
            'upload_progress': 100,
            'phase': 'Done'
        })
        
    except Exception as e:
        err_msg = str(e)
        if "Cancelled" in err_msg:
             print(f"✅ Import task {task_id} marked as cancelled in progress tracker.")
             import_progress[task_id].update({'status': 'cancelled', 'phase': 'Cancelled'})
        else:
             print(f"Import failed: {err_msg}")
             import_progress[task_id].update({'status': 'error', 'error': err_msg})
    finally:
        # Cleanup cancellation flag
        if task_id in import_cancellation_flags:
            import_cancellation_flags.remove(task_id)
        
        # Cleanup
        try:
            if os.path.exists(temp_dir):
                print(f"🧹 Cleaning up temp directory: {temp_dir}")
                shutil.rmtree(temp_dir)
                print(f"✨ Cleanup complete for {task_id}")
        except Exception as e:
            print(f"⚠️ Cleanup error: {e}")


@app.post("/delete-files")
async def delete_files_background(data: dict, background_tasks: BackgroundTasks):
    """Queue file deletion as background task"""
    background_tasks.add_task(
        process_file_deletions,
        data["files"],
        data["bot_token"],
        data["channel_id"],
        data["callback_url"],
        data["user_id"]
    )
    
    return {"status": "queued", "file_count": len(data["files"])}


def process_file_deletions(files: list, bot_token: str, channel_id: str, callback_url: str, user_id: str):
    """Delete files from Telegram and notify backend"""
    deleted_file_ids = []
    
    print(f"🗑️ Starting deletion of {len(files)} files")
    
    for file in files:
        try:
            # Delete from Telegram
            if file.get("telegram_msg_id"):
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/deleteMessage",
                    json={
                        "chat_id": channel_id,
                        "message_id": file["telegram_msg_id"]
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    print(f"✅ Deleted Telegram msg {file['telegram_msg_id']}")
                else:
                    print(f"⚠️ Failed to delete msg {file['telegram_msg_id']}: {response.text}")
            
            # Delete Thumbnail from Telegram
            if file.get("telegram_thumb_msg_id"):
                response = requests.post(
                    f"https://api.telegram.org/bot{bot_token}/deleteMessage",
                    json={
                        "chat_id": channel_id,
                        "message_id": file["telegram_thumb_msg_id"]
                    },
                    timeout=10
                )
                
                if response.status_code == 200:
                    print(f"✅ Deleted Telegram thumbnail msg {file['telegram_thumb_msg_id']}")
                else:
                    print(f"⚠️ Failed to delete thumbnail msg {file['telegram_thumb_msg_id']}: {response.text}")
            
            deleted_file_ids.append(file["id"])
            
        except Exception as e:
            print(f"❌ Failed to delete file {file.get('id', 'unknown')}: {e}")
    
    # Notify backend to cleanup DB
    try:
        print(f"📞 Notifying backend: {len(deleted_file_ids)} files deleted")
        response = requests.post(
            callback_url,
            json={
                "user_id": user_id,
                "deleted_file_ids": deleted_file_ids
            },
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"✅ Backend notified successfully")
        else:
            print(f"⚠️ Backend notification failed: {response.text}")
            
    except Exception as e:
        print(f"❌ Failed to notify backend: {e}")


def get_file_id_fast(message):
    if message.document: return str(message.document.id)
    if message.photo: return str(message.photo.id)
    return None

if __name__ == '__main__':

    # For local development only
    app.run(host='0.0.0.0', port=10000, debug=True)
