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
import mimetypes
import cv2
import numpy as np
from PIL import Image
import io
import base64
from transformers import BlipProcessor, BlipForConditionalGeneration
from deepface import DeepFace
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize

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
    'BACKEND_URL': os.environ.get('BACKEND_URL', 'https://photo-analyzer-16.preview.emergentagent.com'),
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

# Download NLTK stopwords on startup
try:
    nltk.download('stopwords', quiet=True)
    nltk.download('punkt', quiet=True)
    print("NLTK resources downloaded")
except Exception as e:
    print(f"Warning: NLTK download failed: {e}")

# Load BLIP model globally (will be loaded on first use to save startup time)
blip_processor = None
blip_model = None

def load_blip_model():
    """Load BLIP model for image captioning - lazy loading"""
    global blip_processor, blip_model
    if blip_processor is None or blip_model is None:
        print("Loading BLIP model for image description...")
        try:
            blip_processor = BlipProcessor.from_pretrained("Salesforce/blip-image-captioning-base")
            blip_model = BlipForConditionalGeneration.from_pretrained("Salesforce/blip-image-captioning-base")
            print("BLIP model loaded successfully")
        except Exception as e:
            print(f"Failed to load BLIP model: {e}")
            raise
    return blip_processor, blip_model


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


def extract_keywords(description):
    """Extract important keywords from description, removing stopwords"""
    try:
        # Tokenize
        tokens = word_tokenize(description.lower())
        
        # Get stopwords
        try:
            stop_words = set(stopwords.words('english'))
        except:
            # If stopwords not available, use basic list
            stop_words = {'a', 'an', 'the', 'is', 'are', 'was', 'were', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by', 'from'}
        
        # Filter tokens
        keywords = [word for word in tokens if word.isalnum() and word not in stop_words and len(word) > 2]
        
        # Deduplicate while preserving order
        seen = set()
        unique_keywords = []
        for kw in keywords:
            if kw not in seen:
                seen.add(kw)
                unique_keywords.append(kw)
        
        # Return top 10 keywords
        return ', '.join(unique_keywords[:10])
    except Exception as e:
        print(f"Keyword extraction error: {e}")
        return description


def generate_image_description(image_path):
    """Generate image description using BLIP model and extract keywords"""
    try:
        processor, model = load_blip_model()
        
        # Load image
        image = Image.open(image_path).convert('RGB')
        
        # Process image
        inputs = processor(image, return_tensors="pt")
        
        # Generate caption
        out = model.generate(**inputs, max_length=50)
        description = processor.decode(out[0], skip_special_tokens=True)
        
        print(f"Generated description: {description}")
        
        # Extract keywords
        keywords = extract_keywords(description)
        print(f"Extracted keywords: {keywords}")
        
        return keywords
    except Exception as e:
        print(f"Image description error: {e}")
        return None


def detect_faces_and_crop(image_path):
    """Detect faces in image using DeepFace and crop them"""
    try:
        print(f"Detecting faces in {image_path}...")
        
        # Detect faces using DeepFace
        faces = DeepFace.extract_faces(
            img_path=image_path,
            detector_backend='opencv',
            enforce_detection=False,
            align=True
        )
        
        if not faces or len(faces) == 0:
            print("No faces detected")
            return []
        
        print(f"Detected {len(faces)} face(s)")
        
        # Load original image for cropping
        original_image = cv2.imread(image_path)
        height, width = original_image.shape[:2]
        
        result_faces = []
        
        for idx, face_data in enumerate(faces):
            try:
                # Get face region
                facial_area = face_data['facial_area']
                x, y, w, h = facial_area['x'], facial_area['y'], facial_area['w'], facial_area['h']
                confidence = face_data['confidence']
                
                # Skip low confidence detections
                if confidence < 0.5:
                    print(f"Skipping face {idx} due to low confidence: {confidence}")
                    continue
                
                # Add padding around face (20%)
                padding = int(max(w, h) * 0.2)
                x1 = max(0, x - padding)
                y1 = max(0, y - padding)
                x2 = min(width, x + w + padding)
                y2 = min(height, y + h + padding)
                
                # Crop face
                face_crop = original_image[y1:y2, x1:x2]
                
                # Get face embedding/descriptor using DeepFace
                face_embedding = DeepFace.represent(
                    img_path=face_crop,
                    model_name='Facenet',
                    enforce_detection=False
                )[0]['embedding']
                
                result_faces.append({
                    'box': {'x': x, 'y': y, 'width': w, 'height': h},
                    'confidence': confidence,
                    'descriptor': face_embedding,
                    'cropped_image': face_crop
                })
                
                print(f"Processed face {idx}: confidence={confidence:.2f}")
                
            except Exception as e:
                print(f"Error processing face {idx}: {e}")
                continue
        
        return result_faces
        
    except Exception as e:
        print(f"Face detection error: {e}")
        import traceback
        traceback.print_exc()
        return []


def upload_image_to_imgbb(image_data, api_key):
    """Upload image to ImgBB"""
    try:
        # Convert to base64
        if isinstance(image_data, np.ndarray):
            # Convert numpy array to image
            _, buffer = cv2.imencode('.jpg', image_data)
            image_bytes = buffer.tobytes()
            base64_data = base64.b64encode(image_bytes).decode('utf-8')
        else:
            base64_data = image_data
        
        # Upload to ImgBB
        response = requests.post(
            f"https://api.imgbb.com/1/upload?key={api_key}",
            data={'image': base64_data},
            timeout=30
        )
        
        data = response.json()
        if data.get('success'):
            return data['data']['url']
        else:
            print(f"ImgBB upload failed: {data}")
            return None
    except Exception as e:
        print(f"ImgBB upload error: {e}")
        return None


def process_image_background(file_path, file_id, user_id, credentials):
    """Background processing: face detection, image description, face cropping"""
    try:
        print(f"Starting background processing for file {file_id}")
        
        # Check if it's an image
        mime_type = get_mime_type(file_path)
        if not mime_type or not mime_type.startswith('image/'):
            print(f"Not an image file: {mime_type}")
            return
        
        # Generate image description
        image_description = generate_image_description(file_path)
        
        # Detect faces
        detected_faces = detect_faces_and_crop(file_path)
        
        # Process faces - upload cropped faces
        processed_faces = []
        for face_data in detected_faces:
            face_image_url = None
            
            # Upload cropped face to imgbb/cloudinary
            if credentials.get('imgbb_api_key'):
                face_image_url = upload_image_to_imgbb(
                    face_data['cropped_image'],
                    credentials['imgbb_api_key']
                )
            
            if face_image_url:
                processed_faces.append({
                    'box': face_data['box'],
                    'descriptor': face_data['descriptor'],
                    'confidence': float(face_data['confidence']),
                    'face_image_url': face_image_url
                })
        
        # Send data to backend
        send_processed_data_to_backend(
            file_id=file_id,
            user_id=user_id,
            image_description=image_description,
            faces=processed_faces
        )
        
        print(f"Background processing complete for file {file_id}")
        
    except Exception as e:
        print(f"Background processing error: {e}")
        import traceback
        traceback.print_exc()


def send_processed_data_to_backend(file_id, user_id, image_description, faces):
    """Send processed face data and image description to backend"""
    try:
        data = {
            'file_id': file_id,
            'user_id': user_id,
            'image_description': image_description,
            'faces': faces
        }
        
        response = requests.post(
            f"{CONFIG['BACKEND_URL']}/api/worker/process-complete",
            json=data,
            timeout=30
        )
        
        if response.status_code == 200:
            print(f"Successfully sent processed data to backend for file {file_id}")
        else:
            print(f"Failed to send data to backend: {response.status_code} - {response.text}")
            
    except Exception as e:
        print(f"Error sending data to backend: {e}")


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
    file_path = None
    file_id = None
    user_id = None
    credentials = None
    
    try:
        progress = upload_progress[upload_id]
        file_path = progress['file_path']
        file_size = progress['file_size']
        credentials = progress['credentials']
        user_id = credentials.get('user_id')
        
        # Decide whether to use Bot API or Client API
        if file_size <= CONFIG['BOT_API_SIZE_LIMIT']:
            # Use Bot API for files <= 50MB
            upload_with_bot_api(upload_id, file_path, credentials)
        else:
            # Use Telethon Client API for files > 50MB
            upload_with_client_api(upload_id, file_path, credentials)
        
        # After successful upload, trigger background processing
        # Get file_id from backend if available
        if progress['status'] == 'completed':
            # Start background processing in a separate thread
            # Make a copy of file for processing since we'll delete the original
            processing_file_path = file_path + '_processing'
            import shutil
            if os.path.exists(file_path):
                shutil.copy2(file_path, processing_file_path)
                
                # Get file_id from progress (this needs to be set by upload completion)
                # For now, we'll need to track this differently
                # We can get it from the upload response or pass it through
                
                processing_thread = threading.Thread(
                    target=lambda: process_and_cleanup(
                        processing_file_path,
                        upload_id,
                        user_id,
                        credentials
                    )
                )
                processing_thread.daemon = True
                processing_thread.start()
                print(f"Started background processing for upload {upload_id}")
            
    except Exception as e:
        print(f"Background upload error for {upload_id}: {str(e)}")
        import traceback
        traceback.print_exc()
        upload_progress[upload_id]['status'] = 'failed'
        upload_progress[upload_id]['error'] = str(e)
    finally:
        # Cleanup original file after upload (success or failure)
        try:
            if file_path and os.path.exists(file_path):
                os.remove(file_path)
                print(f"Cleaned up temporary file: {file_path}")
        except Exception as e:
            print(f"Error cleaning up file: {str(e)}")


def process_and_cleanup(processing_file_path, upload_id, user_id, credentials):
    """Process image and cleanup - runs in separate thread"""
    try:
        # Wait a bit for backend to create the file entry
        time.sleep(2)
        
        # Get message_id from upload progress
        message_id = upload_progress.get(upload_id, {}).get('message_id')
        if not message_id:
            print(f"Warning: No message_id found for upload {upload_id}, skipping processing")
            return
        
        # Query backend to get file_id by message_id
        file_id = get_file_id_by_message_id(message_id, user_id)
        if not file_id:
            print(f"Warning: Could not find file_id for message_id {message_id}")
            return
        
        process_image_background(processing_file_path, file_id, user_id, credentials)
    finally:
        # Cleanup processing file
        try:
            if os.path.exists(processing_file_path):
                os.remove(processing_file_path)
                print(f"Cleaned up processing file: {processing_file_path}")
        except Exception as e:
            print(f"Error cleaning up processing file: {e}")


def get_file_id_by_message_id(message_id, user_id):
    """Query backend to get file_id by telegram message_id"""
    try:
        response = requests.get(
            f"{CONFIG['BACKEND_URL']}/api/worker/file-by-message/{message_id}",
            params={'user_id': user_id},
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            return data.get('file_id')
        else:
            print(f"Failed to get file_id: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error getting file_id: {e}")
        return None


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
