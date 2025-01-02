from flask import Flask, request, jsonify
from astrapy.db import AstraDB
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize the Astra DB client
db = AstraDB(
    token=os.getenv("ASTRA_DB_APPLICATION_TOKEN"),
    api_endpoint=os.getenv("ASTRA_DB_API_ENDPOINT")
)

@app.route('/get_data', methods=['GET'])
def get_data():
    # Get the channel_id from query parameters
    channel_id = request.args.get('channel_id')
    
    if not channel_id:
        return jsonify({"error": "channel_id parameter is required"}), 400

    # Define your collection name
    collection_name = "data"
    
    try:
        # Get the collection
        collection = db.collection(collection_name)
        
        # Query the collection
        results = collection.find(
            {"channel_id": channel_id}
        )
        
        return results["data"]["documents"][0]
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)