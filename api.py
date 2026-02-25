from fastapi import FastAPI

app = FastAPI()

@app.get("/"
)
def read_root():
    return {"message": "Welcome to the Mini App Bot API"}

@app.post("/mini-app/")
def handle_mini_app_request(request: dict):
    # Your implementation for handling mini app requests goes here
    return {"status": "success", "data": request}