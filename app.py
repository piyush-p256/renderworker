from flask import Flask, request, jsonify, Response, stream_with_context
from flask_cors import CORS
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

app = Flask(__name__)
CORS(app, origins='*', supports_credentials=True)

# Configuration
CONFIG = {
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://chunked-file-fix.preview.emergentagent.com'),
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


@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({"status": "ok", "timestamp": datetime.now().isoformat()})


@app.route('/upload', methods=['POST'])
def upload_file():
    """Handle file upload - stores file temporarily and returns upload ID"""
    try:
        # Get auth token
        auth_token = request.form.get('authToken')
        if not auth_token:
            return jsonify({'error': 'Missing auth token'}), 401
        
        # Get credentials
        credentials = get_credentials(auth_token)
        if not credentials:
            return jsonify({'error': 'Failed to fetch credentials'}), 401
        
        # Get uploaded file
        if 'file' not in request.files:
            return jsonify({'error': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({'error': 'Empty filename'}), 400
        
        # Generate upload ID
        upload_id = hashlib.md5(f"{auth_token}{file.filename}{time.time()}".encode()).hexdigest()
        file_path = os.path.join(CONFIG['UPLOAD_DIR'], upload_id)
        
        # Save file
        file.save(file_path)
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
        
        return jsonify({
            'uploadId': upload_id,
            'size': file_size
        })
        
    except Exception as e:
        print(f"Upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/complete-upload', methods=['POST'])
def complete_upload():
    """Complete the upload by sending to Telegram in background"""
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        
        if not upload_id or upload_id not in upload_progress:
            return jsonify({'error': 'Invalid upload ID'}), 400
        
        progress = upload_progress[upload_id]
        
        if progress['status'] == 'uploading':
            return jsonify({'error': 'Upload already in progress'}), 400
        
        if progress['status'] == 'completed':
            return jsonify({
                'status': 'completed',
                'messageId': progress['message_id'],
                'fileId': progress['file_id']
            })
        
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
        return jsonify({
            'status': 'uploading',
            'uploadId': upload_id,
            'message': 'Upload to Telegram started in background'
        })
        
    except Exception as e:
        print(f"Complete upload error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


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


@app.route('/upload-progress/<upload_id>', methods=['GET'])
def get_upload_progress(upload_id):
    """Get upload progress for a specific upload ID"""
    try:
        if upload_id not in upload_progress:
            return jsonify({'error': 'Upload ID not found'}), 404
        
        progress = upload_progress[upload_id]
        
        return jsonify({
            'status': progress['status'],
            'telegram_progress': progress['telegram_progress'],
            'message_id': progress['message_id'],
            'file_id': progress['file_id'],
            'error': progress['error']
        })
        
    except Exception as e:
        print(f"Progress check error: {str(e)}")
        return jsonify({'error': str(e)}), 500


@app.route('/download', methods=['GET'])
def download_file():
    """Download files from Telegram with Range request support for chunked downloads"""
    try:
        message_id = request.args.get('messageId')
        token = request.args.get('token')
        file_name = request.args.get('fileName', 'file')
        
        if not message_id or not token:
            return jsonify({'error': 'Missing messageId or token'}), 400
        
        # Verify token with backend
        try:
            verify_response = requests.post(
                f"{CONFIG['BACKEND_URL']}/api/worker/verify-download-token",
                data={'token': token},
                timeout=10
            )
            
            if verify_response.status_code != 200:
                return jsonify({'error': 'Invalid or expired token'}), 401
            
            credentials = verify_response.json()
        except Exception as e:
            print(f"Token verification failed: {str(e)}")
            return jsonify({'error': 'Failed to verify token'}), 401
        
        # Get Range header if present
        range_header = request.headers.get('Range')
        
        if range_header:
            # Parse range header: "bytes=0-5242879"
            try:
                range_start, range_end = range_header.replace('bytes=', '').split('-')
                range_start = int(range_start)
                range_end = int(range_end) if range_end else None
            except:
                return jsonify({'error': 'Invalid Range header'}), 416
            
            print(f"Range request: {range_start}-{range_end}")
            
            # Stream specific byte range from Telegram
            return stream_file_range(
                message_id, 
                credentials, 
                file_name, 
                range_start, 
                range_end
            )
        else:
            # Full file download (for small files or legacy support)
            return stream_full_file(message_id, credentials, file_name)
        
    except Exception as e:
        print(f"Download error: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


def stream_file_range(message_id, credentials, file_name, range_start, range_end):
    """Stream a specific byte range from Telegram file"""
    def generate():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = None
        
        try:
            client = TelegramClient(
                StringSession(credentials['telegram_session']),
                int(credentials['telegram_api_id']),
                credentials['telegram_api_hash']
            )
            
            async def download_range():
                await client.connect()
                
                try:
                    # Get channel and message
                    channel = await client.get_entity(int(credentials['channel_id']))
                    message = await client.get_messages(channel, ids=int(message_id))
                    
                    if not message:
                        raise Exception(f"Message {message_id} not found")
                    
                    # Get file size
                    file_size = message.file.size if message.file else 0
                    
                    # Adjust range_end if not specified or exceeds file size
                    actual_end = min(range_end if range_end else file_size - 1, file_size - 1)
                    
                    # Download specific byte range
                    chunks = []
                    bytes_to_download = actual_end - range_start + 1
                    chunk_size = 512 * 1024  # 512KB chunks for smooth streaming
                    
                    print(f"Downloading range {range_start}-{actual_end} ({bytes_to_download} bytes)")
                    
                    async for chunk in client.iter_download(
                        message.media,
                        offset=range_start,
                        limit=bytes_to_download,
                        chunk_size=chunk_size
                    ):
                        chunks.append(chunk)
                    
                    return chunks, file_size, actual_end
                    
                finally:
                    await client.disconnect()
            
            # Download the range
            chunks, file_size, actual_end = loop.run_until_complete(download_range())
            
            # Yield chunks
            for chunk in chunks:
                yield chunk
                
        except Exception as e:
            print(f"Range download error: {str(e)}")
            import traceback
            traceback.print_exc()
            yield b''
            
        finally:
            if loop:
                loop.close()
    
    # Return partial content response (206)
    return Response(
        stream_with_context(generate()),
        status=206,
        mimetype='application/octet-stream',
        headers={
            'Content-Disposition': f'attachment; filename="{file_name}"',
            'Accept-Ranges': 'bytes',
            'Cache-Control': 'no-cache',
            'X-Accel-Buffering': 'no'
        }
    )


def stream_full_file(message_id, credentials, file_name):
    """Stream entire file (for small files or legacy support)"""
    def generate():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        client = None
        
        try:
            client = TelegramClient(
                StringSession(credentials['telegram_session']),
                int(credentials['telegram_api_id']),
                credentials['telegram_api_hash']
            )
            
            async def download_full():
                await client.connect()
                
                try:
                    channel = await client.get_entity(int(credentials['channel_id']))
                    message = await client.get_messages(channel, ids=int(message_id))
                    
                    if not message:
                        raise Exception(f"Message {message_id} not found")
                    
                    chunks = []
                    async for chunk in client.iter_download(message.media, chunk_size=512 * 1024):
                        chunks.append(chunk)
                    
                    return chunks
                    
                finally:
                    await client.disconnect()
            
            chunks = loop.run_until_complete(download_full())
            
            for chunk in chunks:
                yield chunk
                
        except Exception as e:
            print(f"Full download error: {str(e)}")
            import traceback
            traceback.print_exc()
            yield b''
            
        finally:
            if loop:
                loop.close()
    
    return Response(
        stream_with_context(generate()),
        mimetype='application/octet-stream',
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
