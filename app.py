# Render Web Service for TeleStore File Upload with Chunked Upload Support
# Updated for Render Free Tier – supports up to 2GB uploads using background threading
# Version: 2.0 (Optimized for Render Free Tier)

from flask import Flask, request, jsonify
from flask_cors import CORS
import requests
import os
import json
import hashlib
import asyncio
import threading
import time
from datetime import datetime
from pathlib import Path
from telethon import TelegramClient
from telethon.sessions import StringSession

app = Flask(__name__)

# Enable CORS
CORS(app, resources={r"*": {"origins": "*"}})

# Configuration
CONFIG = {
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://chunked-tg-upload.preview.emergentagent.com'),
    'MAX_FILE_SIZE': 2000 * 1024 * 1024,  # 2GB
    'CHUNK_SIZE': 5 * 1024 * 1024,  # 5MB
    'CACHE_DURATION': 3600,
    'UPLOAD_FOLDER': '/tmp/tgdrive_chunks',
}

os.makedirs(CONFIG['UPLOAD_FOLDER'], exist_ok=True)

credentials_cache = {'data': None, 'timestamp': None, 'user_id': None}
upload_sessions = {}

# ------------------- Utility Functions -------------------

def get_credentials(user_id, auth_token):
    """Fetch credentials from backend or return cached."""
    now = datetime.now()

    if (credentials_cache['data'] and credentials_cache['user_id'] == user_id and
            credentials_cache['timestamp'] and
            (now - credentials_cache['timestamp']).total_seconds() < CONFIG['CACHE_DURATION']):
        return credentials_cache['data']

    response = requests.get(
        f"{CONFIG['BACKEND_URL']}/api/worker/credentials",
        headers={'Authorization': f'Bearer {auth_token}'}
    )

    if response.status_code != 200:
        raise Exception(f"Failed to fetch credentials: {response.text}")

    credentials = response.json()
    credentials_cache.update({
        'data': credentials,
        'timestamp': now,
        'user_id': user_id
    })
    return credentials

def get_chunk_path(upload_id, chunk_index):
    return os.path.join(CONFIG['UPLOAD_FOLDER'], f"{upload_id}_chunk_{chunk_index}")

def get_session_metadata_path(upload_id):
    return os.path.join(CONFIG['UPLOAD_FOLDER'], f"{upload_id}_metadata.json")

def cleanup_upload(upload_id):
    """Clean up chunks and metadata."""
    try:
        metadata_path = get_session_metadata_path(upload_id)
        if os.path.exists(metadata_path):
            os.remove(metadata_path)
        for file in os.listdir(CONFIG['UPLOAD_FOLDER']):
            if file.startswith(f"{upload_id}_chunk_"):
                os.remove(os.path.join(CONFIG['UPLOAD_FOLDER'], file))
        if upload_id in upload_sessions:
            del upload_sessions[upload_id]
    except Exception as e:
        print(f"[CLEANUP] Error cleaning upload {upload_id}: {e}")

# ------------------- Telegram Upload Logic -------------------

async def upload_to_telegram_client(file_path, file_name, credentials):
    """Async upload to Telegram via Telethon client."""
    client = None
    try:
        client = TelegramClient(
            StringSession(credentials['telegram_session']),
            int(credentials['telegram_api_id']),
            credentials['telegram_api_hash']
        )
        await client.connect()
        if not await client.is_user_authorized():
            raise Exception("Telegram session not authorized.")
        channel_entity = await client.get_entity(int(credentials['channel_id']))
        message = await client.send_file(
            channel_entity, file_path, caption=f'Uploaded: {file_name}', force_document=True
        )
        await client.disconnect()

        file_id = str(message.document.id) if message.document else None
        return {'messageId': message.id, 'fileId': file_id, 'success': True}

    except Exception as e:
        print(f"[TELETHON ERROR] {e}")
        import traceback; traceback.print_exc()
        if client:
            try:
                await client.disconnect()
            except:
                pass
        raise

# ------------------- Background Upload Task -------------------

def background_upload_task(upload_id, final_file_path, file_name, user_id, auth_token, file_size):
    """Handles Telegram upload in background thread to avoid Render timeout."""
    try:
        print(f"[BG-Upload] Started: {file_name} ({file_size/1024/1024:.2f} MB)")
        credentials = get_credentials(user_id, auth_token)
        BOT_API_LIMIT = 50 * 1024 * 1024  # 50MB

        if file_size > BOT_API_LIMIT:
            print(f"[BG-Upload] Using Telethon client for >50MB file.")
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(
                    upload_to_telegram_client(final_file_path, file_name, credentials)
                )
                message_id = result['messageId']
                file_id = result['fileId']
            finally:
                loop.close()
        else:
            print(f"[BG-Upload] Using Bot API for small file.")
            with open(final_file_path, 'rb') as file_stream:
                files = {'document': (file_name, file_stream, 'application/octet-stream')}
                data = {'chat_id': credentials['channel_id'], 'caption': f'Uploaded: {file_name}'}
                res = requests.post(
                    f"https://api.telegram.org/bot{credentials['bot_token']}/sendDocument",
                    files=files, data=data, timeout=600
                )
            res_json = res.json()
            if not res_json.get('ok'):
                raise Exception(res_json.get('description', 'Telegram upload failed'))
            message_id = res_json['result']['message_id']
            file_id = res_json['result']['document']['file_id']

        # Notify backend
        print(f"[BG-Upload] Notifying backend...")
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
        cleanup_upload(upload_id)
        if os.path.exists(final_file_path):
            os.remove(final_file_path)
        print(f"[BG-Upload] Completed successfully: {file_name}")

    except Exception as e:
        print(f"[BG-Upload] Error: {e}")
        import traceback; traceback.print_exc()

# ------------------- API Routes -------------------

@app.route('/', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'ok',
        'service': 'TeleStore Render Worker',
        'chunk_size': f"{CONFIG['CHUNK_SIZE']//(1024*1024)}MB"
    })

@app.route('/init-upload', methods=['POST'])
def init_upload():
    data = request.get_json()
    upload_id = data.get('uploadId')
    file_name = data.get('fileName')
    total_chunks = data.get('totalChunks')
    file_size = data.get('fileSize', 0)

    if not all([upload_id, file_name, total_chunks]):
        return jsonify({'error': 'Missing required parameters'}), 400

    metadata = {
        'upload_id': upload_id,
        'file_name': file_name,
        'file_size': file_size,
        'total_chunks': total_chunks,
        'received_chunks': [],
        'created_at': datetime.now().isoformat()
    }
    with open(get_session_metadata_path(upload_id), 'w') as f:
        json.dump(metadata, f)
    upload_sessions[upload_id] = metadata
    return jsonify({'success': True, 'uploadId': upload_id})

@app.route('/upload-chunk', methods=['POST'])
def upload_chunk():
    try:
        chunk = request.files.get('chunk')
        upload_id = request.form.get('uploadId')
        chunk_index = int(request.form.get('chunkIndex'))
        total_chunks = int(request.form.get('totalChunks'))
        file_name = request.form.get('fileName')
        user_id = request.form.get('userId')
        auth_token = request.form.get('authToken')
        file_size = int(request.form.get('fileSize', 0))

        if not all([chunk, upload_id, file_name, auth_token]):
            return jsonify({'error': 'Missing required parameters'}), 400

        chunk_path = get_chunk_path(upload_id, chunk_index)
        chunk.save(chunk_path)

        metadata_path = get_session_metadata_path(upload_id)
        metadata = json.load(open(metadata_path)) if os.path.exists(metadata_path) else {}
        received = metadata.get('received_chunks', [])
        if chunk_index not in received:
            received.append(chunk_index)
        metadata.update({
            'upload_id': upload_id,
            'file_name': file_name,
            'file_size': file_size,
            'total_chunks': total_chunks,
            'received_chunks': received,
            'user_id': user_id,
            'auth_token': auth_token
        })
        json.dump(metadata, open(metadata_path, 'w'))
        return jsonify({'success': True, 'received_chunks': len(received), 'total_chunks': total_chunks})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/complete-upload', methods=['POST'])
def complete_upload():
    """Merge chunks and upload via background thread."""
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        if not upload_id:
            return jsonify({'error': 'Missing upload ID'}), 400

        metadata_path = get_session_metadata_path(upload_id)
        if not os.path.exists(metadata_path):
            return jsonify({'error': 'Session not found'}), 404

        metadata = json.load(open(metadata_path))
        file_name = metadata['file_name']
        user_id = metadata['user_id']
        auth_token = metadata['auth_token']
        total_chunks = metadata['total_chunks']

        if len(metadata['received_chunks']) != total_chunks:
            return jsonify({'error': 'Incomplete upload'}), 400

        final_file_path = os.path.join(CONFIG['UPLOAD_FOLDER'], f"{upload_id}_final")
        with open(final_file_path, 'wb') as final_file:
            for i in sorted(metadata['received_chunks']):
                with open(get_chunk_path(upload_id, i), 'rb') as cf:
                    final_file.write(cf.read())

        file_size = os.path.getsize(final_file_path)

        # Start upload thread
        threading.Thread(
            target=background_upload_task,
            args=(upload_id, final_file_path, file_name, user_id, auth_token, file_size),
            daemon=True
        ).start()

        return jsonify({'success': True, 'queued': True, 'message': 'Upload started in background'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/upload-status/<upload_id>', methods=['GET'])
def upload_status(upload_id):
    try:
        metadata_path = get_session_metadata_path(upload_id)
        if not os.path.exists(metadata_path):
            return jsonify({'error': 'Not found'}), 404
        metadata = json.load(open(metadata_path))
        return jsonify({
            'upload_id': upload_id,
            'file_name': metadata['file_name'],
            'total_chunks': metadata['total_chunks'],
            'received_chunks': len(metadata['received_chunks']),
            'complete': len(metadata['received_chunks']) == metadata['total_chunks']
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/cancel-upload', methods=['POST'])
def cancel_upload():
    try:
        data = request.get_json()
        upload_id = data.get('uploadId')
        if not upload_id:
            return jsonify({'error': 'Missing upload ID'}), 400
        cleanup_upload(upload_id)
        return jsonify({'success': True})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ------------------- Auto Ping (Keep Render Awake) -------------------

def auto_ping():
    while True:
        try:
            requests.get("https://your-render-service-name.onrender.com/")
            print("[PING] Keeping service alive")
        except:
            pass
        time.sleep(600)

threading.Thread(target=auto_ping, daemon=True).start()

# ------------------- Main -------------------

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))
