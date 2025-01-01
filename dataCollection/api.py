from flask import Flask, request, jsonify
from astrapy import DataAPIClient
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

app = Flask(__name__)

# Initialize the Astra DB client - using the correct AstraDB class
client = DataAPIClient(os.getenv("ASTRA_DB_APPLICATION_TOKEN"))
db = client.get_database_by_api_endpoint(
    os.getenv("ASTRA_DB_ID"),
)

@app.route('/get_data', methods=['GET'])
def get_data():
    # Get the channel_id from query parameters
    channel_id = request.args.get('channel_id')
    
    if not channel_id:
        return jsonify({"error": "channel_id parameter is required"}), 400
    
    # Define your keyspace and table
    table_name = "data"
    
    # CQL query to fetch data based on the channel_id
    query = f"SELECT * FROM {table_name} WHERE channel_id = %s"
    
    session = None
    try:
        # Create a session and execute query
        session = db.connect()
        rows = session.execute(query, [channel_id])

        # Convert results to list of dictionaries 
        results = []
        for row in rows:
            results.append(dict(row))

        return jsonify({"data": results})

    except Exception as e:
        return jsonify({"error": str(e)}), 500

    finally:
        if session:
            session.shutdown()

if __name__ == '__main__':
    app.run(debug=True)