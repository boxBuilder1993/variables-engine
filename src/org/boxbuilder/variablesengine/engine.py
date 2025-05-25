from typing import Dict, Any, List, Optional, Callable
import logging

from org.boxbuilder.variablesengine.models.project import Project
from org.boxbuilder.variablesengine.models.variable import Variable
from org.boxbuilder.variablesengine.function_registry import REGISTRY

logger = logging.getLogger(__name__)

class Engine:
    def __init__(self):
        self.function_registry = REGISTRY
    
    def execute(
        self, 
        project: Project, 
        inputs: Dict[str, Dict[str, Any]], 
        outputs: Dict[str, List[str]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Execute variable calculations based on inputs and requested outputs.
        
        Args:
            project: The project containing entities and variables definitions
            inputs: Dict structure of {entity_name: {variable_name: value}}
            outputs: Dict structure of {entity_name: [variable_name1, variable_name2, ...]}
            
        Returns:
            Dict structure of {entity_name: {variable_name: calculated_value}}
        """
        # Initialize results dictionary
        results = {}
        
        # Create lookups for variables and entities
        variable_lookup = {v.name: v for v in project.variables}
        entity_lookup = {e.name: e for e in project.entities}
        
        # Create a cache for calculated values
        calculated_values = {}
        
        # Process each entity and requested output variables
        for entity_name, variable_names in outputs.items():
            if entity_name not in entity_lookup:
                logger.warning(f"Entity '{entity_name}' not found in project")
                continue
                
            entity = entity_lookup[entity_name]
            results[entity_name] = {}
            
            # Get input values for this entity
            entity_inputs = inputs.get(entity_name, {})
            
            # Calculate each requested output variable
            for var_name in variable_names:
                if var_name not in variable_lookup:
                    logger.warning(f"Variable '{var_name}' not found in project")
                    continue
                    
                variable = variable_lookup[var_name]
                
                # Skip if the variable doesn't belong to this entity
                if variable.entity_id != entity.id:
                    logger.warning(f"Variable '{var_name}' does not belong to entity '{entity_name}'")
                    continue
                
                # Calculate the variable value
                value = self._calculate_variable(
                    variable=variable,
                    entity_inputs=entity_inputs,
                    variable_lookup=variable_lookup,
                    calculated_values=calculated_values
                )
                
                results[entity_name][var_name] = value
        
        return results
    
    def _calculate_variable(
        self,
        variable: Variable,
        entity_inputs: Dict[str, Any],
        variable_lookup: Dict[str, Variable],
        calculated_values: Dict[str, Any]
    ) -> Any:
        """
        Calculate a variable value, handling dependencies recursively.
        
        Args:
            variable: The variable to calculate
            entity_inputs: Input values for this entity
            variable_lookup: Dictionary of all variables by name
            calculated_values: Cache of already calculated values
            
        Returns:
            The calculated variable value
        """
        # If already calculated, return from cache
        if variable.name in calculated_values:
            return calculated_values[variable.name]
            
        # If it's an input variable, get from inputs
        if variable.is_input:
            if variable.name in entity_inputs:
                value = entity_inputs[variable.name]
                calculated_values[variable.name] = value
                return value
            else:
                # Input variable not provided
                logger.warning(f"Input variable '{variable.name}' not provided")
                return None
        
        # For derived variables, calculate using the function
        if not variable.function_name:
            logger.error(f"Derived variable '{variable.name}' has no function defined")
            return None
            
        # Get the function from registry
        function = self.function_registry.get(variable.function_name)
        if not function:
            logger.error(f"Function '{variable.function_name}' not found for variable '{variable.name}'")
            return None
            
        # Calculate input variable values
        input_values = {}
        for input_var_name in variable.input_variables:
            if input_var_name not in variable_lookup:
                logger.error(f"Input variable '{input_var_name}' not found")
                return None
                
            input_variable = variable_lookup[input_var_name]
            input_value = self._calculate_variable(
                variable=input_variable,
                entity_inputs=entity_inputs,
                variable_lookup=variable_lookup,
                calculated_values=calculated_values
            )
            
            input_values[input_var_name] = input_value
            
        # Execute the function with inputs
        try:
            result = function(**input_values)
            calculated_values[variable.name] = result
            return result
        except Exception as e:
            logger.error(f"Error calculating variable '{variable.name}': {str(e)}")
            return None