# Render Web Service for TeleStore File Upload with Chunked Upload Support
# Deploy this as a Web Service on Render
# Supports large files up to 2GB with 5MB chunks (optimized for Render free tier)

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os
from datetime import datetime, timedelta
import json
import hashlib
from pathlib import Path
import asyncio
import threading
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import DocumentAttributeFilename

app = Flask(__name__)

# Enable CORS for all routes
CORS(app, resources={
    r"/*": {
        "origins": "*",
        "methods": ["GET", "POST", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Configuration - only set BACKEND_URL, credentials will be fetched automatically
CONFIG = {
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://chunked-tg-upload.preview.emergentagent.com'),
    'MAX_FILE_SIZE': 2000 * 1024 * 1024,  # 2GB
    'CHUNK_SIZE': 5 * 1024 * 1024,  # 5MB chunks (safe for Render free tier 10MB limit)
    'CACHE_DURATION': 3600,  # 1 hour in seconds
    'UPLOAD_FOLDER': '/tmp/tgdrive_chunks',  # Temporary folder for chunks
}

# Create upload folder if it doesn't exist
os.makedirs(CONFIG['UPLOAD_FOLDER'], exist_ok=True)

# In-memory cache for credentials
credentials_cache = {
    'data': None,
    'timestamp': None,
    'user_id': None
}

# In-memory tracking of upload sessions
upload_sessions = {}

# Track background upload progress
upload_progress = {}
# Format: {upload_id: {'status': 'uploading'|'completed'|'failed', 'progress': 0-100, 'error': str, 'messageId': int, 'fileId': str}}

def get_credentials(user_id, auth_token):
    """Fetch credentials from backend or return cached version"""
    now = datetime.now()
    
    # Return cached credentials if still valid and for same user
    if (credentials_cache['data'] and 
        credentials_cache['user_id'] == user_id and
        credentials_cache['timestamp'] and
        (now - credentials_cache['timestamp']).total_seconds() < CONFIG['CACHE_DURATION']):
        return credentials_cache['data']
    
    # Fetch fresh credentials from backend
    try:
        response = requests.get(
            f"{CONFIG['BACKEND_URL']}/api/worker/credentials",
            headers={'Authorization': f'Bearer {auth_token}'}
        )
        
        if response.status_code != 200:
            raise Exception(f"Failed to fetch credentials: {response.text}")
        
        credentials = response.json()
        
        # Cache the credentials
        credentials_cache['data'] = credentials
        credentials_cache['timestamp'] = now
        credentials_cache['user_id'] = user_id
        
        return credentials
    except Exception as e:
        # If cache exists, use it even if expired (fallback)
        if credentials_cache['data'] and credentials_cache['user_id'] == user_id:
            print(f'Using expired cache due to fetch error: {e}')
            return credentials_cache['data']
        raise e

def get_chunk_path(upload_id, chunk_index):
    """Get the path for a specific chunk file"""
    return os.path.join(CONFIG['UPLOAD_FOLDER'], f"{upload_id}_chunk_{chunk_index}")

def get_session_metadata_path(upload_id):
    """Get the path for upload session metadata"""
    return os.path.join(CONFIG['UPLOAD_FOLDER'], f"{upload_id}_metadata.json")

def cleanup_upload(upload_id):
    """Clean up all chunks and metadata for an upload"""
    try:
        # Remove metadata file
        metadata_path = get_session_metadata_path(upload_id)
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        
        # Remove all chunk files
        for file in os.listdir(CONFIG['UPLOAD_FOLDER']):
            if file.startswith(f"{upload_id}_chunk_"):
                os.remove(os.path.join(CONFIG['UPLOAD_FOLDER'], file))
        
        # Remove from in-memory sessions
        if upload_id in upload_sessions:
            del upload_sessions[upload_id]
    except Exception as e:
        print(f"Error cleaning up upload {upload_id}: {e}")

@app.route('/', methods=['GET'])
def health_check():
    """Health check endpoint"""
    return jsonify({
        'status': 'ok',
        'service': 'TeleStore Render Upload Service',
        'chunk_size': f"{CONFIG['CHUNK_SIZE'] // (1024 * 1024)}MB",
        'endpoints': ['/init-upload', '/upload-chunk', '/complete-upload', '/upload-status/<id>', '/cancel-upload', '/upload']
    })

@app.route('/init-upload', methods=['POST'])
def init_upload():
    """Initialize a chunked upload session"""
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        file_name = data.get('fileName')
        total_chunks = data.get('totalChunks')
        file_size = data.get('fileSize', 0)
        
        if not all([upload_id, file_name, total_chunks]):
            return jsonify({'error': 'Missing required parameters'}), 400
        
        # Create session metadata
        metadata = {
            'upload_id': upload_id,
            'file_name': file_name,
            'file_size': file_size,
            'total_chunks': total_chunks,
            'received_chunks': [],
            'created_at': datetime.now().isoformat()
        }
        
        # Save metadata
        metadata_path = get_session_metadata_path(upload_id)
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)
        
        # Store in memory
        upload_sessions[upload_id] = metadata
        
        return jsonify({
            'success': True,
            'uploadId': upload_id,
            'message': 'Upload session initialized'
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload-chunk', methods=['POST'])
def upload_chunk():
    """Handle individual chunk uploads"""
    try:
        # Get chunk data
        chunk_file = request.files.get('chunk')
        upload_id = request.form.get('uploadId')
        chunk_index = int(request.form.get('chunkIndex'))
        total_chunks = int(request.form.get('totalChunks'))
        file_name = request.form.get('fileName')
        file_size = int(request.form.get('fileSize'))
        user_id = request.form.get('userId')
        auth_token = request.form.get('authToken')
        
        if not all([chunk_file, upload_id, file_name, auth_token]):
            return jsonify({'error': 'Missing required parameters'}), 400
        
        # Save chunk to disk
        chunk_path = get_chunk_path(upload_id, chunk_index)
        chunk_file.save(chunk_path)
        
        # Update session metadata
        metadata_path = get_session_metadata_path(upload_id)
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
        else:
            metadata = {
                'upload_id': upload_id,
                'file_name': file_name,
                'file_size': file_size,
                'total_chunks': total_chunks,
                'received_chunks': [],
                'user_id': user_id,
                'auth_token': auth_token,
                'created_at': datetime.now().isoformat()
            }
        
        # Mark chunk as received
        if chunk_index not in metadata['received_chunks']:
            metadata['received_chunks'].append(chunk_index)
        
        # Save updated metadata
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f)
        
        # Check if all chunks received
        all_received = len(metadata['received_chunks']) == total_chunks
        
        return jsonify({
            'success': True,
            'chunk_index': chunk_index,
            'received_chunks': len(metadata['received_chunks']),
            'total_chunks': total_chunks,
            'complete': all_received
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def background_upload_to_telegram(upload_id, file_path, file_name, file_size, credentials, user_id, auth_token):
    """Background task to upload file to Telegram (runs in separate thread)"""
    try:
        print(f"[{upload_id}] Background upload started for: {file_name} ({file_size} bytes)")
        upload_progress[upload_id] = {
            'status': 'uploading',
            'progress': 0,
            'error': None,
            'messageId': None,
            'fileId': None
        }
        
        BOT_API_LIMIT = 50 * 1024 * 1024  # 50MB
        
        if file_size > BOT_API_LIMIT:
            # Use Telegram Client API for large files
            print(f"[{upload_id}] Using Telethon Client API (file > 50MB)")
            
            # Run async upload in new event loop
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    upload_to_telegram_client_async(upload_id, file_path, file_name, credentials)
                )
                message_id = result['messageId']
                file_id = result['fileId']
            finally:
                loop.close()
        else:
            # Use Bot API for small files
            print(f"[{upload_id}] Using Bot API (file <= 50MB)")
            with open(file_path, 'rb') as file_stream:
                files = {
                    'document': (file_name, file_stream, 'application/octet-stream')
                }
                data = {
                    'chat_id': credentials['channel_id'],
                    'caption': f'Uploaded: {file_name}'
                }
                
                telegram_response = requests.post(
                    f"https://api.telegram.org/bot{credentials['bot_token']}/sendDocument",
                    files=files,
                    data=data,
                    timeout=300
                )
            
            telegram_result = telegram_response.json()
            
            if not telegram_result.get('ok'):
                raise Exception(telegram_result.get('description', 'Telegram upload failed'))
            
            message_id = telegram_result['result']['message_id']
            
            # Extract file_id
            result = telegram_result['result']
            file_id = (
                result.get('document', {}).get('file_id') or
                result.get('video', {}).get('file_id') or
                result.get('audio', {}).get('file_id') or
                (result.get('photo', [{}])[0].get('file_id') if result.get('photo') else None)
            )
            
            if not file_id:
                raise Exception('Failed to get file_id from Telegram response')
        
        # Update progress to completed
        upload_progress[upload_id] = {
            'status': 'completed',
            'progress': 100,
            'error': None,
            'messageId': message_id,
            'fileId': file_id
        }
        
        print(f"[{upload_id}] Upload completed successfully")
        
        # Notify backend
        try:
            requests.post(
                f"{CONFIG['BACKEND_URL']}/api/webhook/upload",
                json={
                    'userId': user_id,
                    'fileName': file_name,
                    'messageId': message_id,
                    'fileId': file_id,
                    'size': file_size,
                    'mimeType': 'application/octet-stream',
                },
                timeout=30
            )
            print(f"[{upload_id}] Backend notified")
        except Exception as e:
            print(f"[{upload_id}] Failed to notify backend: {e}")
        
        # Clean up file after successful upload
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                print(f"[{upload_id}] Cleaned up file: {file_path}")
        except Exception as e:
            print(f"[{upload_id}] Failed to cleanup file: {e}")
            
    except Exception as e:
        print(f"[{upload_id}] Upload failed: {str(e)}")
        import traceback
        traceback.print_exc()
        
        upload_progress[upload_id] = {
            'status': 'failed',
            'progress': 0,
            'error': str(e),
            'messageId': None,
            'fileId': None
        }
        
        # Clean up file on error
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except:
            pass


async def upload_to_telegram_client_async(upload_id, file_path, file_name, credentials):
    """Async function to upload file using Telegram Client API"""
    client = None
    try:
        print(f"[{upload_id}] Starting Telethon upload")
        
        # Validate credentials
        if not credentials.get('telegram_session'):
            raise Exception("telegram_session is missing")
        if not credentials.get('telegram_api_id'):
            raise Exception("telegram_api_id is missing")
        if not credentials.get('telegram_api_hash'):
            raise Exception("telegram_api_hash is missing")
        if not credentials.get('channel_id'):
            raise Exception("channel_id is missing")
        
        # Update progress
        upload_progress[upload_id]['progress'] = 10
        
        # Create client
        client = TelegramClient(
            StringSession(credentials['telegram_session']),
            int(credentials['telegram_api_id']),
            credentials['telegram_api_hash']
        )
        
        await client.connect()
        
        if not await client.is_user_authorized():
            raise Exception("Telegram session not authorized")
        
        print(f"[{upload_id}] Connected to Telegram")
        upload_progress[upload_id]['progress'] = 20
        
        # Resolve channel
        channel_id = credentials['channel_id']
        try:
            channel_entity = await client.get_entity(int(channel_id))
        except Exception as e:
            if str(channel_id).startswith('-100'):
                bare_id = int(str(channel_id).replace('-100', ''))
                channel_entity = await client.get_entity(bare_id)
            else:
                raise Exception(f"Could not resolve channel: {str(e)}")
        
        print(f"[{upload_id}] Channel resolved")
        upload_progress[upload_id]['progress'] = 30
        
        # Upload with progress callback
        def progress_callback(current, total):
            if total > 0:
                percent = 30 + int((current / total) * 60)  # 30-90%
                upload_progress[upload_id]['progress'] = percent
                print(f"[{upload_id}] Upload progress: {percent}% ({current}/{total})")
        
        message = await client.send_file(
            channel_entity,
            file_path,
            caption=f'Uploaded: {file_name}',
            force_document=True,
            progress_callback=progress_callback
        )
        
        upload_progress[upload_id]['progress'] = 95
        
        await client.disconnect()
        
        message_id = message.id
        file_id = str(message.document.id) if message.document else None
        
        print(f"[{upload_id}] Upload completed: message_id={message_id}")
        
        return {
            'messageId': message_id,
            'fileId': file_id,
            'success': True
        }
        
    except Exception as e:
        print(f"[{upload_id}] Telethon error: {str(e)}")
        if client:
            try:
                await client.disconnect()
            except:
                pass
        raise

@app.route('/complete-upload', methods=['POST'])
def complete_upload():
    """Merge chunks and start background upload to Telegram"""
    upload_id = None
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        
        if not upload_id:
            return jsonify({'error': 'Missing upload ID'}), 400
        
        # Load metadata
        metadata_path = get_session_metadata_path(upload_id)
        if not os.path.exists(metadata_path):
            return jsonify({'error': 'Upload session not found'}), 404
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        file_name = metadata['file_name']
        total_chunks = metadata['total_chunks']
        user_id = metadata['user_id']
        auth_token = metadata['auth_token']
        
        # Verify all chunks received
        if len(metadata['received_chunks']) != total_chunks:
            return jsonify({
                'error': 'Not all chunks received',
                'received': len(metadata['received_chunks']),
                'total': total_chunks
            }), 400
        
        # Merge chunks into final file
        final_file_path = os.path.join(CONFIG['UPLOAD_FOLDER'], f"{upload_id}_final")
        with open(final_file_path, 'wb') as final_file:
            for chunk_index in sorted(metadata['received_chunks']):
                chunk_path = get_chunk_path(upload_id, chunk_index)
                if os.path.exists(chunk_path):
                    with open(chunk_path, 'rb') as chunk_file:
                        final_file.write(chunk_file.read())
        
        # Get file size
        file_size = os.path.getsize(final_file_path)
        
        # Get credentials
        credentials = get_credentials(user_id, auth_token)
        
        # Clean up chunks (keep merged file for background upload)
        for chunk_index in metadata['received_chunks']:
            chunk_path = get_chunk_path(upload_id, chunk_index)
            if os.path.exists(chunk_path):
                os.remove(chunk_path)
        
        # Remove metadata file
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        
        # Start background upload in separate thread
        upload_thread = threading.Thread(
            target=background_upload_to_telegram,
            args=(upload_id, final_file_path, file_name, file_size, credentials, user_id, auth_token),
            daemon=True
        )
        upload_thread.start()
        
        print(f"[{upload_id}] Background upload thread started")
        
        # Return immediately - client will poll for progress
        return jsonify({
            'success': True,
            'uploadId': upload_id,
            'message': 'Upload started in background',
            'checkProgressAt': f'/upload-progress/{upload_id}'
        })
        
    except Exception as e:
        print(f"Error in complete_upload: {str(e)}")
        import traceback
        traceback.print_exc()
        
        # Try to clean up on error
        if upload_id:
            try:
                cleanup_upload(upload_id)
            except:
                pass
        
        return jsonify({'error': str(e)}), 500

@app.route('/upload-status/<upload_id>', methods=['GET'])
def upload_status(upload_id):
    """Get status of chunk upload session (before merging)"""
    try:
        metadata_path = get_session_metadata_path(upload_id)
        if not os.path.exists(metadata_path):
            return jsonify({'error': 'Upload session not found'}), 404
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        return jsonify({
            'upload_id': upload_id,
            'file_name': metadata['file_name'],
            'total_chunks': metadata['total_chunks'],
            'received_chunks': len(metadata['received_chunks']),
            'received_chunk_indices': metadata['received_chunks'],
            'complete': len(metadata['received_chunks']) == metadata['total_chunks']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload-progress/<upload_id>', methods=['GET'])
def upload_progress_status(upload_id):
    """Get progress of background Telegram upload"""
    try:
        if upload_id not in upload_progress:
            return jsonify({'error': 'Upload not found or not started'}), 404
        
        progress_data = upload_progress[upload_id]
        
        return jsonify({
            'uploadId': upload_id,
            'status': progress_data['status'],
            'progress': progress_data['progress'],
            'error': progress_data['error'],
            'messageId': progress_data['messageId'],
            'fileId': progress_data['fileId']
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/cancel-upload', methods=['POST'])
def cancel_upload():
    """Cancel an upload and clean up chunks"""
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        
        if not upload_id:
            return jsonify({'error': 'Missing upload ID'}), 400
        
        cleanup_upload(upload_id)
        
        return jsonify({'success': True, 'message': 'Upload cancelled'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# Legacy endpoint for small files (backwards compatibility)
@app.route('/upload', methods=['POST'])
def upload_file():
    """Legacy upload endpoint for small files"""
    try:
        file = request.files.get('file')
        user_id = request.form.get('userId')
        auth_token = request.form.get('authToken')
        file_name = request.form.get('fileName') or file.filename
        
        if not file:
            return jsonify({'error': 'No file provided'}), 400
        
        if not auth_token:
            return jsonify({'error': 'Auth token required'}), 400
        
        # Check file size
        file.seek(0, 2)
        file_size = file.tell()
        file.seek(0)
        
        if file_size > CONFIG['MAX_FILE_SIZE']:
            return jsonify({'error': 'File too large (max 2GB)'}), 400
        
        # Fetch credentials from backend (or use cache)
        credentials = get_credentials(user_id, auth_token)
        
        # Upload to Telegram
        files = {
            'document': (file_name, file.read(), file.content_type)
        }
        data = {
            'chat_id': credentials['channel_id'],
            'caption': f'Uploaded: {file_name}'
        }
        
        telegram_response = requests.post(
            f"https://api.telegram.org/bot{credentials['bot_token']}/sendDocument",
            files=files,
            data=data
        )
        
        telegram_result = telegram_response.json()
        
        if not telegram_result.get('ok'):
            raise Exception(telegram_result.get('description', 'Telegram upload failed'))
        
        message_id = telegram_result['result']['message_id']
        
        result = telegram_result['result']
        file_id = (
            result.get('document', {}).get('file_id') or
            result.get('video', {}).get('file_id') or
            result.get('audio', {}).get('file_id') or
            (result.get('photo', [{}])[0].get('file_id') if result.get('photo') else None)
        )
        
        if not file_id:
            raise Exception('Failed to get file_id from Telegram response')
        
        # Notify backend
        requests.post(
            f"{CONFIG['BACKEND_URL']}/api/webhook/upload",
            json={
                'userId': user_id,
                'fileName': file_name,
                'messageId': message_id,
                'fileId': file_id,
                'size': file_size,
                'mimeType': file.content_type,
            }
        )
        
        return jsonify({
            'success': True,
            'messageId': message_id,
            'fileId': file_id,
            'fileName': file_name,
        })
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
