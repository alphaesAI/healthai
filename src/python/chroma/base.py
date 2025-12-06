from chromadb import HttpClient

class Chroma:
    def __init__(self, host="localhost", port=8001):
        self.client = HttpClient(host=host, port=port)

    def create_collection(self, name: str):
        return self.client.get_or_create_collection(name=name)
    
    def list_collections(self):
        return self.client.list_collections()
    
    def get_collection(self, name):
        return self.client.get_collection(name=name)
    
    def add_documents(self, collection_name, ids, documents):
        collection = self.get_collection(collection_name)
        return collection.add(ids=ids, documents=documents)
    
    def update_documents(self, collection_name, ids, documents=None):
        collection = self.get_collection(collection_name)
        return collection.update(ids=ids, documents=documents)
    
    def delete_documents(self, collection_name, ids):
        collection = self.get_collection(collection_name)
        return collection.delete(ids=ids)
    
    def get_documents(self, collection_name):
        collection = self.get_collection(collection_name)
        return collection.get()
    
    def close():
        pass