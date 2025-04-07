from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
from threading import Thread, Lock
from queue import Queue

'Setup for FastAPI'
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

'The JSON Body or Payload'
class Item(BaseModel):
    userId : str
    driverIds : List[str]

'Multithread access Queue'
class MessageQueue():
    def __init__(self):
        self.q = Queue()
        self.lock = Lock()
    
    def insert(self, item):
        with self.lock:
            self.q.put(item)
    
    def get(self):
        with self.lock:
            if self.q.empty():
                return None
            curItem = self.q.get()
            return curItem

messageQueue = MessageQueue()

'Thread Functions'
def pushItem(item):
    global messageQueue
    messageQueue.insert(item)

def getItem(result):
    global messageQueue
    result["response"] = messageQueue.get()

'''
Producer Endpoint:
POST with a payload JSON
{userId : string, driverIds : List[string]}
'''
@app.post("/producer/")
async def consume_data(item : Item):
    curThread = Thread(target = pushItem, args = (item, ))
    curThread.start()
    curThread.join()
    return {"message" : "Data Inserted"}

'''
Consumer Endpoint:
- returns JSON payload
{ userId : string, driverIds : List[string]}
- If Queue empty it returns
{"message" : "Queue Empty"}
'''
@app.get("/consumer/")
async def consume_data():
    result = {}
    curThread = Thread(target = getItem, args = (result, ))
    curThread.start()
    curThread.join()
    if result["response"] == None:
        return {"message" : "Queue Empty"}
    return result["response"]