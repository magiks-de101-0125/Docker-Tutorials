from fastapi import FastAPI, Form
import redis

# Connect to Redis
r = redis.Redis(host='redis', port=6379)

app = FastAPI()

FILE_PATH = "/data/storage.txt"  # Path to store user inputs in the volume

@app.post("/submit/")
def submit_input(user_input: str = Form(...)):
    # Save the user input to the file (local file storage)
    with open(FILE_PATH, "a") as f:
        f.write(user_input + "\n")
    
    # Save input to Redis as well
    r.set('last_input', user_input)
    
    return {"status": "Input received", "input": user_input}

@app.get("/")
def read_inputs():
    # Read all inputs from the file (local file storage)
    try:
        with open(FILE_PATH, "r") as f:
            content = f.read().splitlines()
    except FileNotFoundError:
        content = []
    
    # Get the last input from Redis
    last_input = r.get('last_input')
    
    return {"saved_inputs": content, "last_input_from_redis": last_input.decode('utf-8') if last_input else None}