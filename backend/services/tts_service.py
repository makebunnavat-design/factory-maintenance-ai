import os
import logging
import hashlib
import time
import glob
from typing import Optional

logger = logging.getLogger("[TTS_SERVICE]")

# Cache settings
CACHE_MAX_AGE_HOURS = 24  # ลบไฟล์ที่เก่ากว่า 24 ชั่วโมง
CACHE_MAX_FILES = 50     # เก็บไฟล์สูงสุด 100 ไฟล์

# Try PyThaiTTS (Thai-specific TTS)
try:
    from pythaitts import TTS
    import soundfile as sf
    import numpy as np
    HAS_PYTHAITTS = True
    logger.info("✅ PyThaiTTS library available")
except ImportError as e:
    HAS_PYTHAITTS = False
    logger.warning(f"PyThaiTTS not found: {e}")

class TTSManager:
    _instance: Optional["TTSManager"] = None
    _lock = None

    def __init__(self):
        # Find frontend/static directory
        backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        project_root = os.path.dirname(backend_dir)
        
        # Check if we're in Docker or local
        if os.path.exists("/app/frontend/static"):
            self.static_dir = "/app/frontend/static"
            self.model_dir = "/app/models"
        else:
            self.static_dir = os.path.join(project_root, "frontend", "static")
            self.model_dir = os.path.join(project_root, "backend", "models")
            
        self.output_dir = os.path.join(self.static_dir, "tts_cache")
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Clean up old cache files on startup
        self._cleanup_cache()
        
        # Initialize TTS backend
        self.backend = None
        self.model_name = None
        self.tts_model = None
        
        if HAS_PYTHAITTS:
            try:
                logger.info("Initializing PyThaiTTS...")
                
                # Initialize PyThaiTTS with VachanaTTS model (best for Thai)
                self.tts_model = TTS(pretrained="vachana")
                
                self.backend = "pythaitts"
                self.model_name = "PyThaiTTS"
                logger.info("✅ Successfully initialized PyThaiTTS")
                
            except Exception as e:
                logger.error(f"Failed to initialize PyThaiTTS: {e}")
                logger.exception("Full traceback:")
                self.backend = None
                self.model_name = None
                self.tts_model = None
        else:
            logger.warning("❌ PyThaiTTS not available")
            self.backend = None
            self.model_name = None

    @classmethod
    def instance(cls) -> "TTSManager":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance
    def generate_speech(self, text: str, speaker_wav: Optional[str] = None) -> Optional[str]:
        """
        Generates speech using PyThaiTTS and returns the relative path to the WAV file.
        """
        if not self.backend or not self.tts_model:
            logger.warning("No TTS backend available")
            return None

        # Create a unique filename based on the text
        text_hash = hashlib.md5(text.encode('utf-8')).hexdigest()
        filename = f"{text_hash}.wav"
        filepath = os.path.join(self.output_dir, filename)

        # Return cached file if exists
        if os.path.exists(filepath):
            logger.info(f"Using cached TTS file: {filename}")
            # Update file access time for cache management
            os.utime(filepath, None)
            return f"/static/tts_cache/{filename}"

        # Clean cache before generating new file
        self._cleanup_cache()

        try:
            logger.info(f"Generating PyThaiTTS for: {text[:50]}...")
            
            if self.backend == "pythaitts":
                # Generate audio using PyThaiTTS with VachanaTTS
                # Available voices: th_f_1 (female), th_m_1 (male)
                # You can change speaker_idx to switch voices
                audio_data = self.tts_model.tts(text, speaker_idx="th_f_1", return_type="waveform")
                
                # Convert to numpy array if needed
                if hasattr(audio_data, 'numpy'):
                    audio_data = audio_data.numpy()
                elif not isinstance(audio_data, np.ndarray):
                    audio_data = np.array(audio_data)
                
                # Ensure audio is 1D
                if audio_data.ndim > 1:
                    audio_data = audio_data.squeeze()
                
                # Normalize audio to [-1, 1] range
                if len(audio_data) > 0:
                    max_val = np.max(np.abs(audio_data))
                    if max_val > 0:
                        audio_data = audio_data / max_val
                
                # Save audio file (22050 Hz is standard)
                sample_rate = getattr(self.tts_model, 'sample_rate', 22050)
                sf.write(filepath, audio_data, sample_rate)
                
                # Check if file was created successfully
                if os.path.exists(filepath) and os.path.getsize(filepath) > 0:
                    logger.info(f"✅ Generated PyThaiTTS: {filename} ({len(audio_data)/sample_rate:.2f}s)")
                    return f"/static/tts_cache/{filename}"
                else:
                    logger.error(f"Failed to create TTS file: {filepath}")
                    return None
            
            return None
            
        except Exception as e:
            logger.error(f"PyThaiTTS generation failed: {e}")
            logger.exception("Full traceback:")
            return None

    def get_available_speakers(self):
        """Get available TTS speakers"""
        if self.backend == "pythaitts" and self.tts_model:
            try:
                # PyThaiTTS may have multiple models/voices
                if hasattr(self.tts_model, 'get_models'):
                    return self.tts_model.get_models()
                else:
                    return ["default"]
            except:
                return ["default"]
        return []

    def _cleanup_cache(self):
        """Clean up old TTS cache files"""
        try:
            cache_files = glob.glob(os.path.join(self.output_dir, "*.wav"))
            current_time = time.time()
            
            # Remove files older than CACHE_MAX_AGE_HOURS
            old_files = []
            for file_path in cache_files:
                file_age_hours = (current_time - os.path.getmtime(file_path)) / 3600
                if file_age_hours > CACHE_MAX_AGE_HOURS:
                    old_files.append(file_path)
            
            # Remove oldest files if we exceed CACHE_MAX_FILES
            if len(cache_files) > CACHE_MAX_FILES:
                # Sort by modification time (oldest first)
                cache_files.sort(key=lambda x: os.path.getmtime(x))
                files_to_remove = len(cache_files) - CACHE_MAX_FILES
                old_files.extend(cache_files[:files_to_remove])
            
            # Remove duplicate entries and delete files
            old_files = list(set(old_files))
            for file_path in old_files:
                try:
                    os.remove(file_path)
                    logger.info(f"Removed old TTS cache file: {os.path.basename(file_path)}")
                except OSError as e:
                    logger.warning(f"Failed to remove cache file {file_path}: {e}")
                    
            if old_files:
                logger.info(f"Cleaned up {len(old_files)} old TTS cache files")
                
        except Exception as e:
            logger.error(f"Cache cleanup failed: {e}")

# Export instance
tts_manager = TTSManager.instance