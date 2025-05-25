from dotenv import load_dotenv
from org.boxbuilder.variablesengine.models.project import Project
from flask import Flask, request, jsonify
from typing import Dict, List
from pydantic import BaseModel

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

# Add these new model classes with your existing imports
class SaveVariableValuesRequest(BaseModel):
    values: Dict[str, Dict[str, str]]

class GetVariableValuesRequest(BaseModel):
    context: Dict[str, Dict[str, str]]
    required_variables: Dict[str, List[str]]

# Add these new endpoints after your existing endpoints

@app.route('/api/saveVariableValues', methods=['POST'])
def save_variable_values():
    try:
        data = request.get_json()
        values = SaveVariableValuesRequest(values=data)
        
        # Convert the nested dictionary to a format suitable for storage
        values_to_save = []
        for entity, vars_dict in values.values.items():
            for variable, value in vars_dict.items():
                values_to_save.append({
                    "entity": entity,
                    "variable": variable,
                    "value": value
                })
        
        # TODO: Implement database storage logic here
        
        return jsonify({
            "success": True,
            "message": f"Successfully saved {len(values_to_save)} variable values"
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

@app.route('/api/getVariableValues', methods=['POST'])
def get_variable_values():
    try:
        data = request.get_json()
        request_data = GetVariableValuesRequest(
            context=data.get('context', {}),
            required_variables=data.get('required_variables', {})
        )
        
        # Initialize response dictionary
        result = {}
        
        # For each entity and its required variables
        for entity, variables in request_data.required_variables.items():
            result[entity] = {}
            
            for variable in variables:
                # Check context first
                if entity in request_data.context and variable in request_data.context[entity]:
                    result[entity][variable] = request_data.context[entity][variable]
                else:
                    # TODO: Implement database lookup logic here
                    result[entity][variable] = None
        
        return jsonify({
            "success": True,
            "values": result
        }), 200
        
    except Exception as e:
        return jsonify({
            "success": False,
            "error": str(e)
        }), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001, debug=True)