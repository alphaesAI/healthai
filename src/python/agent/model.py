import re

from enum import Enum

from smolagents import ChatMessage, Model, get_clean_message_list, tool_role_conversions
"""
ChatMessage: let's you store a message's role and its content in a clean object.
Model: base class for models used by smolagents
get_clean_message_list: takes list of messages and returns ready-to-send list of plain dicts.
tool_role_conversations: normalizes roles for consistency
get_tool_call_from_text: extracts tool + arguments from the text the model generates.
remove_stop_sequences: cleans up the model output by removing stop sequences.
"""
from smolagents.models import get_tool_call_from_text, remove_stop_sequences

from ..pipeline import LLM

class PipelineModel(Model):
    def __init__(self, path=None, method=None, **kwargs):
        """
        path: model location like 'gpt4' or a local path
        method: how to run the llm or which backend method to use. 'chat' or 'completion'
        self.llm: LLM object
        self.llm.generator: actual model generator inside that object
        self.model_id: label or reference to the model
        """
        self.llm = path if isinstance(path, LLM) else LLM(path, method, **kwargs)
        self.maxlength = 8192

        self.model_id = self.llm.generator.path

        super().__init__(flatten_messages_as_text=not self.llm.isvision(), **kwargs)

    def generate(self, messages, stop_sequences=None, response_format=None, tools_to_call_from=None, **kwargs):
        messages = self.clean(messages)
        response = self.llm(messages, maxlength=self.maxlength, stop=stop_sequences, **kwargs)

        if stop_sequences is not None:
            response = remove_stop_sequences(response, stop_sequences)

        message = ChatMessage(role="assistant", content=response)

        if tools_to_call_from:
            message.tool_calls = [
                get_tool_call_from_text(
                    re.sub(r".*?Action:(.*?\n\}).*", r"\1", response, flags=re.DOTALL), self.tool_name_key, self.tool_arguments_key
                )
            ]

        return message
    
    def parameters(self, maxlength):
        self.maxlength = maxlength
    
    def clean(self, messages):
        messages = get_clean_message_list(messages, role_conversions=tool_role_conversions, flatten_messages_as_text=self.flatten_messages_as_text)

        for message in messages:
            if "role" in message:
                message["role"] = message["role"].value if isinstance(message["role"], Enum) else message["role"]
            
        return messages