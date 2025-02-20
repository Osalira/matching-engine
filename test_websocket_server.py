from flask import Flask
from flask_sock import Sock
import json
import logging

# Configure logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
sock = Sock(app)

@sock.route('/ws')
def handle_websocket(ws):
    """Handle WebSocket connections"""
    logger.info("New WebSocket connection established")
    
    while True:
        try:
            # Receive message from client
            message = ws.receive()
            logger.info(f"Received message: {message}")
            
            try:
                data = json.loads(message)
                
                # Handle heartbeat messages
                if data.get('type') == 'heartbeat':
                    response = json.dumps({
                        'type': 'heartbeat',
                        'status': 'ok'
                    })
                    logger.info(f"Sending heartbeat response: {response}")
                    ws.send(response)
                    
                # Handle other message types
                else:
                    logger.info(f"Received data: {json.dumps(data, indent=2)}")
                    # Echo the message back for testing
                    ws.send(json.dumps({
                        'type': 'echo',
                        'data': data
                    }))
                    
            except json.JSONDecodeError:
                logger.warning(f"Received invalid JSON: {message}")
                
        except Exception as e:
            logger.error(f"Error handling WebSocket message: {str(e)}")
            break

@app.route('/health')
def health_check():
    """Health check endpoint"""
    return {'status': 'healthy'}, 200

if __name__ == '__main__':
    port = 4013  # Use a different port for testing
    logger.info(f"Starting WebSocket server on port {port}")
    app.run(host='0.0.0.0', port=port, debug=True) 