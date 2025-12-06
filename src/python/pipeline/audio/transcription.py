"""
Note:
    audio: 
        - What you pass in at the very start
        - Could be a file path, a numpy array, a tuple of file paths, or even an open file object.
    raw:
        - After the file is read/decoded, you get the actual waveform data as a numpy array of samples.
        - Always numeric audio samples.
    speech:
        - Later in the flow - usually a list of (raw, rate) tuples ready to send into process().
"""

import numpy as np

try:
    import soundfile as sf
    
    from .signal import Signal, SCIPY
    TRANSCRIPTION = SCIPY
except (ImportError, OSError):
    TRANSCRIPTION = False

from ..hfpipeline import HFPipeline

class Transcription(HFPipeline):
    def __init__(self, path=None, quantize=False, gpu=True, model=None, **kwargs):
        """
        path: points to a local model you downloaded
        quantize: if True, use a smaller, faster model with lower precision
        gpu: if True, try to run on GPU(faster); otherwise CPU
        model: remote huggingface model
        **kwargs: any extra arguments to pass to the parent HFPipeline constructor
        """
        if not TRANSCRIPTION:
            raise ImportError(
                'Transcription pipeline is not available - install "scipy & soundfile" to enable'
            )
        
        super().__init__("automatic-speech-recognition", path, quantize, gpu, model, **kwargs)

    def __call__(self, audio, rate=None, chunk=10, join=True, **kwargs):
        values = [audio] if self.isaudio(audio) else audio
        speech = self.read(values, rate)
        results = self.batchprocess(speech, chunk, **kwargs) if chunk and not join else self.process(speech, chunk, **kwargs)
        return results[0] if self.isaudio(audio) else results

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
    
    def process(self, speech, chunk, **kwargs):
        results = []
        for result in self.pipeline([self.convert(*x) for x in speech], chunk_length_s=chunk, ignore_warning=True, generate_kwargs=kwargs):
            results.append(self.clean(result["text"]))

        return results
    
    def batchprocess(self, speech, chunk, **kwargs):
        results = []
        for raw, rate in speech:
            segments = self.segments(raw, rate, chunk)
            sresults = []
            for x, result in enumerate(self.pipeline([self.convert(*x) for x in segments], generate_kwargs=kwargs)):
                sresults.append({"text": self.clean(result["text"]), "raw": segments[x][0], "rate": segments[x][1]})

            results.append(sresults)
        
        return results
    
    def segments(self, raw, rate, chunk):
        """
        this method splits a single audio array into smaller time-sized pieces.
        
        args:
            raw: the input audio waveform (Numpy array)
            rate: how many samples per second (the sample rate)
            chunk: a number (float or int) representing seconds of audio per piece.
        
        returns:
            [(segment-array, rate),]"""
        segments = []
        for segment in self.batch(raw, rate * chunk):
            segments.append((segment, rate))
        
        return segments
    
    def convert(self, raw, rate):
        """
        converts stereo to mono and sets the target rate
        
        args:
            raw: the input audio waveform (numpy array)
            rate: how many samples per second
        
        returns:
            one single dictionary for the one (raw, rate) pair you pass in.
        """
        raw = Signal.mono(raw)
        
        target = self.pipeline.feature_extractor.sampling_rate
        return {"raw": Signal.resample(raw, rate, target), "sampling_rate": target}
    
    def clean(self, text):
        text = text.strip()
        
        return text.capitalize() if text.issupper() else text
