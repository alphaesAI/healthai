from huggingface_hub import InferenceClient

from ..base import Pipeline
import os
import numpy as np
import soundfile as sf

class SpeechToText(Pipeline):
    """
    Adapter for huggingface inference API
    """
    def __init__(self, provider=None, api_key=None):
        self.provider = provider or os.getenv("HF_PROVIDER")
        self.api_key = api_key or os.getenv("HF_TOKEN")
        self.client = InferenceClient(provider=provider, api_key=api_key)

    def __call__(self, audio_file, model):
        return self.speech_to_text(audio_file, model)

    def speech_to_text(self, audio_file, model):
        result = self.client.automatic_speech_recognition(audio_file, model=model)
        return result

    def isaudio(self, audio):
        return isinstance(audio, (str, tuple, np.ndarray)) or hasattr(audio, "read")    

    def read(self, audio, rate):
        speech = []
        for x in audio:
            if isinstance(x, str) or hasattr(x, "read"):
                raw, samplerate = sf.read(x)
            elif isinstance(x, tuple):
                raw, samplerate = x
            else:
                raw, samplerate = x, rate

            speech.append((raw, samplerate))
        
        return speech

# class TextToSpeech(Pipeline):
#     def __init__(self, provider, api_key):
#         self.client = InferenceClient(provider=provider, api_key=api_key)

#     def __call__(self, model, text):
#         return self.text_to_speech(model, text)

#     def text_to_speech(self, model, text):
#         result = self.client.text_to_speech(model=model, text=text)
#         return result    

# class SpeechToText(Pipeline):
#     """
#     Adapter for huggingface inference API
#     """
#     def __init__(self, model_id: str, hf_token: str, target_rate: int = 16000):
#         super().__init__()
#         self.client = InferenceClient(token=hf_token)
#         self.model_id = model_id
#         self.target_rate = target_rate

#     def __call__(self, audio, rate=None, chunk=10, join=True, **kwargs):
#         values = [audio] if self.isaudio(audio) else audio
#         speech = self.read(values, rate)
#         results = self.batchprocess(speech, chunk, **kwargs) if chunk and not join else self.process(speech, chunk, **kwargs)
#         return results[0] if self.isaudio(audio) else results




#     def process(self, speech, chunk, **kwargs):
#         results = []
#         for x in speech:
#             converted = self.convert(*x)
#             audio_bytes = self._to_wav_bytes(converted["raw"], converted["sampling_rate"])
#             text = self._call_model(audio_bytes)
#             results.append(self.clean(text))
#         return results
    
#     def batchprocess(self, speech, chunk, **kwargs):
#         results = []
#         for raw, rate in speech:
#             segments = self.segments(raw, rate, chunk)
#             sresults = []
#             for seg in segments:
#                 converted = self.convert(*seg)
#                 audio_bytes = self._to_wav_bytes(converted["raw"], converted["sampling_rate"])
#                 text = self._call_model(audio_bytes, **kwargs)
#                 sresults.append({"text": self.clean(text), "raw": seg[0], "rate": seg[1]})
#             results.append(sresults)
#         return results
    
#     def _to_wav_bytes(self, raw, rate):
#         buf = io.BytesIO()
#         sf.write(buf, raw, rate, format="WAV")
#         buf.seek(0)
#         return buf.read()

#     def _call_model(self, audio_bytes, **kwargs):
#         response = self.client.automatic_speech_recognition(
#             model=self.model_id,
#             audio=audio_bytes,
#             **kwargs
#         )
#         return response.get("text", "")

#     def segments(self, raw, rate, chunk):
#         """
#         Split a single audio array into smaller time-sized pieces.
        
#         Args:
#             raw: numpy array of audio samples
#             rate: samples per second
#             chunk: seconds per segment

#         Returns:
#             List of tuples: [(segment-array, rate), ...]
#         """
#         segments = []
#         chunk_size = int(rate * chunk)
#         for start in range(0, len(raw), chunk_size):
#             end = start + chunk_size
#             segments.append((raw[start:end], rate))
#         return segments

#     def convert(self, raw, rate):
#         """
#         Convert audio to mono and resample to target rate.

#         Args:
#             raw: numpy array of audio samples
#             rate: current sample rate
#             target_rate: desired sample rate (default 16kHz)

#         Returns:
#             Dictionary with 'raw' and 'sampling_rate'
#         """
#         raw = Signal.mono(raw)
#         raw = Signal.resample(raw, rate, self.target_rate)
#         return {"raw": raw, "sampling_rate": self.target_rate}        
    
#     def clean(self, text):
#         text = text.strip()
        
#         return text.capitalize() if text.issupper() else text
