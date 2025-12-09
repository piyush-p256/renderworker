from fastapi import FastAPI, Request, HTTPException, Form, File, UploadFile
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import os
import requests
import hashlib
import json
import time
import shutil
from typing import List, Optional
from datetime import datetime
import threading
from telethon import TelegramClient
from telethon.sessions import StringSession
import asyncio
from collections import defaultdict
from contextlib import asynccontextmanager
import mimetypes

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
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://894728515923.ngrok-free.app'),
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


def get_mime_type(filename):
    """Get MIME type from filename"""
    mime_type, _ = mimetypes.guess_type(filename)
    if mime_type:
        return mime_type
    # Default to octet-stream if unknown
    return 'application/octet-stream'


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
        file_path = os.path.join(CONFIG['UPLOAD_DIR'], upload_id)
        
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
            'error': None
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
                'error': None
            }
        
        # Track received chunk
        if 'received_chunks' not in upload_progress[uploadId]:
             upload_progress[uploadId]['received_chunks'] = set()
             
        upload_progress[uploadId]['received_chunks'].add(chunkIndex)
        
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
        raise HTTPException(status_code=404, detail='Upload not found')
        
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
            final_file_path = os.path.join(CONFIG['UPLOAD_DIR'], f"{upload_id}_final")
            
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
             print(f"Complete upload failed: Upload already in progress for {upload_id}")
             raise HTTPException(status_code=400, detail='Upload to Telegram already in progress')
        
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
            'status': progress.get('status'),
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
    
    # Get file size first for headers - single connection approach
    client = TelegramClient(
        StringSession(credentials['telegram_session']),
        int(credentials['telegram_api_id']),
        credentials['telegram_api_hash']
    )
    
    try:
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
        
        # Create async generator for streaming
        async def generate_chunks():
            """Async generator that yields chunks from Telegram"""
            try:
                # Stream chunks with 1MB chunk size for efficiency
                chunk_size = 1024 * 1024  # 1MB chunks
                downloaded = 0
                
                async for chunk in client.iter_download(
                    message.media,
                    offset=range_start,
                    limit=bytes_to_send,
                    chunk_size=chunk_size
                ):
                    # Check if client disconnected
                    if await request.is_disconnected():
                        print(f"Client disconnected during download, stopping...")
                        break
                    
                    downloaded += len(chunk)
                    if downloaded % (5 * 1024 * 1024) == 0:  # Log every 5MB
                        print(f"Streamed: {downloaded}/{bytes_to_send} bytes")
                    
                    yield chunk
                
                print(f"Range streaming complete: {downloaded} bytes")
                    
            except Exception as e:
                print(f"Streaming error: {str(e)}")
                import traceback
                traceback.print_exc()
                raise
            finally:
                # Disconnect after streaming is complete
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
                'Cache-Control': 'no-cache',
                'X-Accel-Buffering': 'no'
            }
        )
    except Exception as e:
        # Ensure we disconnect on error
        if client:
            await client.disconnect()
        raise


async def stream_full_file(request: Request, message_id, credentials, file_name):
    """Stream entire file using pure async generator"""
    
    # Get file size first for Content-Length header
    client_temp = TelegramClient(
        StringSession(credentials['telegram_session']),
        int(credentials['telegram_api_id']),
        credentials['telegram_api_hash']
    )
    await client_temp.connect()
    channel_temp = await client_temp.get_entity(int(credentials['channel_id']))
    message_temp = await client_temp.get_messages(channel_temp, ids=int(message_id))
    
    if not message_temp or not message_temp.file:
        await client_temp.disconnect()
        raise Exception(f"Message {message_id} not found or has no file")
    
    file_size = message_temp.file.size
    await client_temp.disconnect()
    
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
    
    # Detect MIME type from filename
    mime_type = get_mime_type(file_name)
    
    return StreamingResponse(
        generate_chunks(),
        media_type=mime_type,
        headers={
            'Content-Disposition': f'inline; filename="{file_name}"',  # inline for browser playback
            'Accept-Ranges': 'bytes',
            'Content-Length': str(file_size),
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


if __name__ == '__main__':
    # For local development only
    app.run(host='0.0.0.0', port=10000, debug=True)
