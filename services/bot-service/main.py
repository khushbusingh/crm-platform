from fastapi import FastAPI
import uvicorn

app = FastAPI(title='Bot Service')

@app.get('/health')
def health():
    return {'status': 'ok', 'service': 'bot-service'}


if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
