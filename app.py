from fastapi import FastAPI, Request, HTTPException, Form, File, UploadFile
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import requests
import hashlib
import json
import time
from datetime import datetime
import threading
from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager

# Lifespan for cleanup
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    # Cleanup on shutdown if needed

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
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://buffer-playback-fix.preview.emergentagent.com'),
    'MAX_CHUNK_SIZE': 50 * 1024 * 1024,  # 50MB per chunk
    'UPLOAD_DIR': '/tmp/uploads',
    'BOT_API_SIZE_LIMIT': 50 * 1024 * 1024,  # 50MB - use Bot API up to 50MB
}

# In-memory storage for credentials cache and upload progress
credentials_cache = {}
upload_progress = {}
upload_locks = defaultdict(threading.Lock)

# Create upload directory if it doesn't exist
os.makedirs(CONFIG['UPLOAD_DIR'], exist_ok=True)

print(f"Worker started with BACKEND_URL: {CONFIG['BACKEND_URL']}")


def get_credentials(auth_token):
    """Fetch and cache user credentials from backend"""
    # Check cache first (cache for 1 hour)
    cache_key = hashlib.md5(auth_token.encode()).hexdigest()
    if cache_key in credentials_cache:
        cached_data, cached_time = credentials_cache[cache_key]
        if time.time() - cached_time < 3600:  # 1 hour cache
            return cached_data
    
    # Fetch from backend
    try:
        response = requests.get(
            f"{CONFIG['BACKEND_URL']}/api/worker/credentials",
            headers={'Authorization': f'Bearer {auth_token}'},
            timeout=10
        )
        
        if response.status_code == 200:
            credentials = response.json()
            credentials_cache[cache_key] = (credentials, time.time())
            return credentials
        else:
            print(f"Failed to fetch credentials: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching credentials: {str(e)}")
        return None


@app.get('/health')
async def health_check():
    """Health check endpoint"""
    return {"status": "ok", "timestamp": datetime.now().isoformat()}


@app.post('/upload')
async def upload_file(
    authToken: str = Form(...),
    file: UploadFile = File(...)
):
    """Handle file upload - stores file temporarily and returns upload ID"""
    try:
        # Get credentials
        credentials = get_credentials(authToken)
        if not credentials:
            raise HTTPException(status_code=401, detail='Failed to fetch credentials')
        
        if not file.filename:
            raise HTTPException(status_code=400, detail='Empty filename')
        
        # Generate upload ID
        upload_id = hashlib.md5(f"{authToken}{file.filename}{time.time()}".encode()).hexdigest()
        file_path = os.path.join(CONFIG['UPLOAD_DIR'], upload_id)
        
        # Save file
        with open(file_path, 'wb') as buffer:
            content = await file.read()
            buffer.write(content)
        
        file_size = os.path.getsize(file_path)
        
        # Initialize upload progress
        upload_progress[upload_id] = {
            'status': 'uploaded',
            'file_path': file_path,
            'file_size': file_size,
            'file_name': file.filename,
            'credentials': credentials,
            'telegram_progress': 0,
            'message_id': None,
            'file_id': None,
            'error': None
        }
        
        return {
            'uploadId': upload_id,
            'size': file_size
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"Upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post('/complete-upload')
async def complete_upload(request: Request):
    """Complete the upload by sending to Telegram in background"""
    try:
        data = await request.json()
        upload_id = data.get('uploadId')
        
        if not upload_id or upload_id not in upload_progress:
            raise HTTPException(status_code=400, detail='Invalid upload ID')
        
        progress = upload_progress[upload_id]
        
        if progress['status'] == 'uploading':
            raise HTTPException(status_code=400, detail='Upload already in progress')
        
        if progress['status'] == 'completed':
            return {
                'status': 'completed',
                'messageId': progress['message_id'],
                'fileId': progress['file_id']
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
        
        print(f"Bot API upload completed: message_id={telegram_result['message_id']}, file_id={file_id}")
        
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
            progress['message_id'] = result['message_id']
            progress['file_id'] = result['file_id']
            
            print(f"Telethon upload completed: message_id={result['message_id']}, file_id={result['file_id']}")
            
        finally:
            loop.close()
            
    except Exception as e:
        print(f"Client API upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


async def upload_to_telegram_client(file_path, file_name, credentials, upload_id):
    """Upload file to Telegram using Telethon with progress tracking"""
    client = None
    try:
        # Initialize Telethon client
        client = TelegramClient(
            StringSession(credentials['telegram_session']),
            int(credentials['telegram_api_id']),
            credentials['telegram_api_hash']
        )
        
        await client.connect()
        print("Telethon client connected")
        
        # Get channel entity
        channel_id = int(credentials['channel_id'])
        channel = await client.get_entity(channel_id)
        print(f"Channel entity resolved: {channel.id}")
        
        # Progress callback
        def progress_callback(current, total):
            progress_percent = int((current / total) * 100)
            upload_progress[upload_id]['telegram_progress'] = progress_percent
            if progress_percent % 10 == 0:  # Log every 10%
                print(f"Upload progress: {progress_percent}% ({current}/{total} bytes)")
        
        # Upload file
        print(f"Starting Telethon upload: {file_name}")
        message = await client.send_file(
            channel,
            file_path,
            caption=file_name,
            progress_callback=progress_callback
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
            'status': progress['status'],
            'telegram_progress': progress['telegram_progress'],
            'message_id': progress['message_id'],
            'file_id': progress['file_id'],
            'error': progress['error']
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
    
    async def generate_chunks():
        """Async generator that yields chunks from Telegram"""
        client = None
        try:
            client = TelegramClient(
                StringSession(credentials['telegram_session']),
                int(credentials['telegram_api_id']),
                credentials['telegram_api_hash']
            )
            
            await client.connect()
            
            # Get channel and message
            channel = await client.get_entity(int(credentials['channel_id']))
            message = await client.get_messages(channel, ids=int(message_id))
            
            if not message or not message.file:
                raise Exception(f"Message {message_id} not found or has no file")
            
            # Get file size
            file_size = message.file.size
            
            # Adjust range_end if not specified or exceeds file size
            actual_end = min(range_end if range_end is not None else file_size - 1, file_size - 1)
            bytes_to_download = actual_end - range_start + 1
            
            print(f"Downloading range {range_start}-{actual_end} ({bytes_to_download} bytes) from file size {file_size}")
            
            # Stream chunks with larger chunk size (1MB) for efficiency
            chunk_size = 1024 * 1024  # 1MB chunks (increased from 512KB)
            downloaded = 0
            
            async for chunk in client.iter_download(
                message.media,
                offset=range_start,
                limit=bytes_to_download,
                chunk_size=chunk_size
            ):
                # Check if client disconnected
                if await request.is_disconnected():
                    print(f"Client disconnected during download, stopping...")
                    break
                
                downloaded += len(chunk)
                if downloaded % (5 * 1024 * 1024) == 0:  # Log every 5MB
                    print(f"Downloaded: {downloaded}/{bytes_to_download} bytes")
                
                yield chunk
            
            print(f"Download complete: {downloaded} bytes")
                
        except Exception as e:
            print(f"Streaming error: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            if client:
                await client.disconnect()
    
    # Return partial content response (206) with StreamingResponse
    return StreamingResponse(
        generate_chunks(),
        status_code=206,
        media_type='application/octet-stream',
        headers={
            'Content-Disposition': f'attachment; filename="{file_name}"',
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


async def stream_full_file(request: Request, message_id, credentials, file_name):
    """Stream entire file using pure async generator"""
    
    async def generate_chunks():
        """Async generator for full file download"""
        client = None
        try:
            client = TelegramClient(
                StringSession(credentials['telegram_session']),
                int(credentials['telegram_api_id']),
                credentials['telegram_api_hash']
            )
            
            await client.connect()
            
            channel = await client.get_entity(int(credentials['channel_id']))
            message = await client.get_messages(channel, ids=int(message_id))
            
            if not message or not message.file:
                raise Exception(f"Message {message_id} not found or has no file")
            
            print(f"Downloading full file: {file_name} ({message.file.size} bytes)")
            
            # Stream chunks with 1MB size
            downloaded = 0
            async for chunk in client.iter_download(message.media, chunk_size=1024 * 1024):
                # Check if client disconnected
                if await request.is_disconnected():
                    print(f"Client disconnected during download, stopping...")
                    break
                
                downloaded += len(chunk)
                if downloaded % (10 * 1024 * 1024) == 0:  # Log every 10MB
                    print(f"Downloaded: {downloaded}/{message.file.size} bytes")
                
                yield chunk
            
            print(f"Full download complete: {downloaded} bytes")
                
        except Exception as e:
            print(f"Streaming error: {str(e)}")
            import traceback
            traceback.print_exc()
            raise
        finally:
            if client:
                await client.disconnect()
    
    return StreamingResponse(
        generate_chunks(),
        media_type='application/octet-stream',
        headers={
            'Content-Disposition': f'attachment; filename="{file_name}"',
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


if __name__ == '__main__':
    # For local development only
    app.run(host='0.0.0.0', port=10000, debug=True)
