import os
from flask import Flask, jsonify, request
from dotenv import load_dotenv
from org.boxbuilder.variablesengine.models.project import Project

# Load environment variables
load_dotenv()

app = Flask(__name__)

@app.route('/api/project/<id>', methods=['GET'])
def get_project(id: str):
    """
    Get a project by its ID
    Args:
        id: The project identifier
    Returns:
        Project object
    """
    # TODO: Implement database retrieval
    # This is just a placeholder response
    project = Project(
        id=id,
        name="Sample Project"
    )
    return jsonify(project.model_dump())

@app.route('/api/project', methods=['POST', 'PUT'])
def save_project():
    """
    Create or update a project
    Request body should contain a Project object
    Returns:
        Saved Project object
    """
    project_data = request.get_json()
    project = Project(**project_data)
    
    # TODO: Implement database save/update logic
    
    return jsonify(project.model_dump())

if __name__ == '__main__':
    app.run(host='0.0.0.0', debug=True)