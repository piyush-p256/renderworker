# Render Web Service for TeleStore File Upload with Chunked Upload Support
# Deploy this as a Web Service on Render
# Supports large files up to 2GB with 50MB chunks

from flask import Flask, request, jsonify
import requests
import os
from datetime import datetime, timedelta
import json
import hashlib
from pathlib import Path

app = Flask(__name__)

# Configuration - only set BACKEND_URL, credentials will be fetched automatically
CONFIG = {
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://mega-upload.preview.emergentagent.com'),
    'MAX_FILE_SIZE': 2000 * 1024 * 1024,  # 2GB
    'CHUNK_SIZE': 8 * 1024 * 1024,  # 50MB chunks
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

@app.route('/upload-chunk', methods=['POST', 'OPTIONS'])
def upload_chunk():
    """Handle individual chunk uploads"""
    # Handle CORS
    if request.method == 'OPTIONS':
        response = jsonify({})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response
    
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
        
        response_data = {
            'success': True,
            'chunk_index': chunk_index,
            'received_chunks': len(metadata['received_chunks']),
            'total_chunks': total_chunks,
            'complete': all_received
        }
        
        response = jsonify(response_data)
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
        
    except Exception as e:
        response = jsonify({'error': str(e)})
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response, 500

@app.route('/complete-upload', methods=['POST', 'OPTIONS'])
def complete_upload():
    """Merge chunks and upload to Telegram"""
    # Handle CORS
    if request.method == 'OPTIONS':
        response = jsonify({})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response
    
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
        
        # Upload to Telegram using streaming to avoid memory issues
        with open(final_file_path, 'rb') as file_stream:
            files = {
                'document': (file_name, file_stream, 'application/octet-stream')
            }
            data = {
                'chat_id': credentials['channel_id'],
                'caption': f'Uploaded: {file_name}'
            }
            
            # Use streaming upload with timeout for large files
            telegram_response = requests.post(
                f"https://api.telegram.org/bot{credentials['bot_token']}/sendDocument",
                files=files,
                data=data,
                timeout=600  # 10 minute timeout for large files
            )
        
        telegram_result = telegram_response.json()
        
        if not telegram_result.get('ok'):
            raise Exception(telegram_result.get('description', 'Telegram upload failed'))
        
        message_id = telegram_result['result']['message_id']
        
        # Extract file_id from Telegram response
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
                'mimeType': 'application/octet-stream',
            },
            timeout=30
        )
        
        # Clean up all chunks and temp files
        cleanup_upload(upload_id)
        
        # Remove final merged file
        if os.path.exists(final_file_path):
            os.remove(final_file_path)
        
        response = jsonify({
            'success': True,
            'messageId': message_id,
            'fileId': file_id,
            'fileName': file_name,
        })
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
        
    except Exception as e:
        # Try to clean up on error
        if 'upload_id' in locals():
            cleanup_upload(upload_id)
        
        response = jsonify({'error': str(e)})
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response, 500

@app.route('/upload-status/<upload_id>', methods=['GET', 'OPTIONS'])
def upload_status(upload_id):
    """Get status of an upload session"""
    if request.method == 'OPTIONS':
        response = jsonify({})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'GET, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response
    
    try:
        metadata_path = get_session_metadata_path(upload_id)
        if not os.path.exists(metadata_path):
            response = jsonify({'error': 'Upload session not found'})
            response.headers['Access-Control-Allow-Origin'] = '*'
            return response, 404
        
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        response = jsonify({
            'upload_id': upload_id,
            'file_name': metadata['file_name'],
            'total_chunks': metadata['total_chunks'],
            'received_chunks': len(metadata['received_chunks']),
            'received_chunk_indices': metadata['received_chunks'],
            'complete': len(metadata['received_chunks']) == metadata['total_chunks']
        })
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
        
    except Exception as e:
        response = jsonify({'error': str(e)})
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response, 500

@app.route('/cancel-upload', methods=['POST', 'OPTIONS'])
def cancel_upload():
    """Cancel an upload and clean up chunks"""
    if request.method == 'OPTIONS':
        response = jsonify({})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response
    
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        
        if not upload_id:
            return jsonify({'error': 'Missing upload ID'}), 400
        
        cleanup_upload(upload_id)
        
        response = jsonify({'success': True, 'message': 'Upload cancelled'})
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
        
    except Exception as e:
        response = jsonify({'error': str(e)})
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response, 500

# Legacy endpoint for small files (backwards compatibility)
@app.route('/upload', methods=['POST', 'OPTIONS'])
def upload_file():
    """Legacy upload endpoint for small files"""
    # Handle CORS
    if request.method == 'OPTIONS':
        response = jsonify({})
        response.headers['Access-Control-Allow-Origin'] = '*'
        response.headers['Access-Control-Allow-Methods'] = 'POST, OPTIONS'
        response.headers['Access-Control-Allow-Headers'] = 'Content-Type, Authorization'
        return response
    
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
        
        response = jsonify({
            'success': True,
            'messageId': message_id,
            'fileId': file_id,
            'fileName': file_name,
        })
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response
        
    except Exception as e:
        response = jsonify({'error': str(e)})
        response.headers['Access-Control-Allow-Origin'] = '*'
        return response, 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
