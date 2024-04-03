import ollama

class ChatBot:
    def __init__(self, model: str):
        self.model = model
        self.message_queue = []

    def generate(self, msg: str, options=None):
        """
        in the mode of generation, chat without history
        """
        stream = ollama.generate(model=self.model, 
            prompt=msg,
            stream=True,
            options=options
          )
    
        response = ""
        for chunk in stream:
            part = chunk['response']
            print(part, end='', flush=True)
            response = response + part
    
    def chat(self, msg: str, options=None):
      self.message_queue.append(
        {
          'role': 'user',
          'content': msg,
        }
      )
      stream = ollama.chat(model=self.model, 
        messages=self.message_queue,
        stream=True,
        options=options
      )
    
      response = ""
      for chunk in stream:
        part = chunk['message']['content']
        print(part, end='', flush=True)
        response = response + part
    
      self.message_queue.append(
        {
          'role': 'assistant',
          'content': response,
        }
      )
    
    def clear_message_queue(self):
        self.message_queue.clear()