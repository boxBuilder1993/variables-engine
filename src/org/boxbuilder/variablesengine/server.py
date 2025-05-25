import os
from flask import Flask, jsonify
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

@app.route('/api/status', methods=['GET'])
def status():
    """Simple API endpoint that returns the service status"""
    return jsonify({
        'status': 'online',
        'service': 'variables-engine',
        'version': '0.1.0'
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)