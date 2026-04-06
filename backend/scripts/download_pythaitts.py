#!/usr/bin/env python3
"""
Setup PyThaiTTS for offline Thai TTS
"""
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def setup_pythaitts():
    """Setup PyThaiTTS"""
    
    try:
        from pythaitts import TTS
        logger.info("PyThaiTTS library available")
    except ImportError as e:
        logger.error(f"PyThaiTTS library not available: {e}")
        return False
    
    try:
        logger.info("📥 Initializing PyThaiTTS with VachanaTTS...")
        
        # Initialize PyThaiTTS with VachanaTTS model (this will download models if needed)
        tts = TTS(pretrained="vachana")
        
        logger.info("✅ PyThaiTTS initialized successfully!")
        
        # Test the model
        logger.info("🧪 Testing PyThaiTTS...")
        test_text = "สวัสดีครับ ระบบ TTS ภาษาไทยทำงานได้แล้ว"
        
        try:
            audio = tts.tts(test_text, speaker_idx="th_f_1", return_type="waveform")
            
            if audio is not None and len(audio) > 0:
                logger.info("✅ PyThaiTTS test successful!")
                return True
            else:
                logger.error("❌ PyThaiTTS test failed - no audio generated")
                return False
        except Exception as test_error:
            logger.error(f"❌ PyThaiTTS test failed: {test_error}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Failed to setup PyThaiTTS: {e}")
        return False

if __name__ == "__main__":
    success = setup_pythaitts()
    if success:
        print("✅ PyThaiTTS setup completed!")
    else:
        print("❌ PyThaiTTS setup failed!")