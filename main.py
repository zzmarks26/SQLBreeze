from fastapi import FastAPI
from routes import transpile, metadata, optimize, format
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(transpile.router, prefix="/sqlbreeze")
app.include_router(metadata.router, prefix="/sqlbreeze")
app.include_router(optimize.router, prefix="/sqlbreeze")
app.include_router(format.router, prefix="/sqlbreeze")


@app.get("/")
def read_root():
    return {"message": "Welcome to the SQLBreeze API Service"}


#TODO Add route for formatting SQL
