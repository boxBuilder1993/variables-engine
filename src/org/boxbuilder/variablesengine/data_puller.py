from typing import Dict, List, Set, Any, Optional
import logging
from dataclasses import dataclass
from collections import defaultdict

from org.boxbuilder.variablesengine.models.project import Project
from org.boxbuilder.variablesengine.models.variable import Variable
from org.boxbuilder.database.postgres.query_helper import QueryHelper

logger = logging.getLogger(__name__)

@dataclass
class VariableDependency:
    variable: Variable
    dependencies: Set[str]  # Set of variable names this variable depends on
    foreign_keys: Dict[str, str]  # Map of variable name to referenced entity name

class DataPuller:
    def __init__(self, query_helper: QueryHelper):
        self.query_helper = query_helper
        
    def build_dependency_graph(self, project: Project) -> Dict[str, VariableDependency]:
        """Build a graph of variable dependencies including foreign key relationships."""
        graph = {}
        
        # First pass: collect all variables and their direct dependencies
        for variable in project.variables:
            dependencies = set(variable.input_variables)
            foreign_keys = {}
            
            # Check for foreign key relationships in metadata
            if variable.metadata.get('foreign_key'):
                foreign_keys[variable.name] = variable.metadata['foreign_key']['entity']
            
            graph[variable.name] = VariableDependency(
                variable=variable,
                dependencies=dependencies,
                foreign_keys=foreign_keys
            )
            
        return graph
    
    def get_required_variables(
        self,
        project: Project,
        requested_outputs: Dict[str, List[str]],
        provided_inputs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Set[str]]:
        """
        Determine all variables needed to calculate the requested outputs.
        Returns a dict mapping entity names to sets of required variable names.
        """
        graph = self.build_dependency_graph(project)
        required_vars = defaultdict(set)
        
        # Add all requested output variables
        for entity_name, var_names in requested_outputs.items():
            required_vars[entity_name].update(var_names)
        
        # Add all provided input variables
        for entity_name, inputs in provided_inputs.items():
            required_vars[entity_name].update(inputs.keys())
        
        # Process each entity's required variables
        for entity_name, var_names in list(required_vars.items()):
            to_process = set(var_names)
            while to_process:
                var_name = to_process.pop()
                if var_name not in graph:
                    continue
                    
                # Add dependencies
                deps = graph[var_name].dependencies
                to_process.update(deps - required_vars[entity_name])
                required_vars[entity_name].update(deps)
                
                # Add foreign key referenced variables
                for fk_var, ref_entity in graph[var_name].foreign_keys.items():
                    if fk_var in required_vars[entity_name]:
                        # Add the referenced entity's ID variable
                        required_vars[ref_entity].add('id')
        
        return dict(required_vars)
    
    async def pull_data(
        self,
        project: Project,
        requested_outputs: Dict[str, List[str]],
        provided_inputs: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """
        Pull all necessary data from the database to calculate the requested outputs.
        Returns a dict mapping entity names to dicts of variable values.
        """
        required_vars = self.get_required_variables(project, requested_outputs, provided_inputs)
        
        # Build a single optimal query
        query = self._build_optimal_query(project, required_vars, provided_inputs)
        
        try:
            # Execute query
            query_results = await self.query_helper.get_query_results_as_dictionaries(
                database_name="variables_engine",
                query=query,
                params=[],
                output_column_name_data_type_mapping={
                    "entity_name": "TEXT",
                    "entity_instance_id": "TEXT",
                    "variable_name": "TEXT",
                    "value": "JSON"
                }
            )
            
            # Process results
            results = defaultdict(dict)
            for row in query_results:
                entity_name = row['entity_name']
                var_name = row['variable_name']
                if var_name in required_vars[entity_name]:  # Only include requested variables
                    results[entity_name][var_name] = row['value']
            
            return dict(results)
            
        except Exception as e:
            logger.error(f"Error pulling data: {str(e)}")
            return {}
    
    def _build_optimal_query(
        self,
        project: Project,
        required_vars: Dict[str, Set[str]],
        input_filters: Dict[str, Dict[str, Any]]
    ) -> str:
        """Build a single optimal SQL query to fetch all required data. Only use dependency resolution and input variables."""
        # Start with base query using a CTE for each entity
        entity_ctes = []
        
        for entity_name, var_names in required_vars.items():
            entity = next((e for e in project.entities if e.name == entity_name), None)
            if not entity:
                continue
                
            # Build CTE for this entity
            cte_name = f"{entity_name.lower()}_data"
            entity_ctes.append(f"""
            {cte_name} AS (
                SELECT 
                    '{entity_name}' as entity_name,
                    vv.entity_instance_id,
                    v.name as variable_name,
                    vv.value
                FROM variables_engine.variable_values vv
                JOIN variables_engine.variables v ON v.id = vv.variable_id
                WHERE v.entity_id = '{entity.id}'
                AND v.name IN ({', '.join(f"'{name}'" for name in var_names)})
            )""")
        
        # Combine all CTEs
        query = "WITH " + ",\n".join(entity_ctes)
        
        # Add final SELECT that combines all entity data
        query += """
        SELECT * FROM (
        """
        
        # UNION ALL all entity CTEs
        query += "\nUNION ALL\n".join(
            f"SELECT * FROM {entity_name.lower()}_data"
            for entity_name in required_vars.keys()
        )
        
        # Close the query
        query += """
        ) combined_data
        ORDER BY entity_name, entity_instance_id, variable_name;
        """
        
        return query 