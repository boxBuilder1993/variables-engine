-- Set the search path for convenience
SET search_path TO variables_engine, public;

-- Indexes for entities table
CREATE INDEX idx_entities_name ON entities(name);
CREATE INDEX idx_entities_project_id ON entities(project_id);

-- Indexes for entity_values table
CREATE INDEX idx_entity_values_entity_id ON entity_values(entity_id);
CREATE INDEX idx_entity_values_instance_id ON entity_values(instance_id);
CREATE INDEX idx_entity_values_name ON entity_values(name);
CREATE INDEX idx_entity_values_metadata ON entity_values USING GIN (metadata);

-- Indexes for variables table
CREATE INDEX idx_variables_name ON variables(name);
CREATE INDEX idx_variables_entity_id ON variables(entity_id);
CREATE INDEX idx_variables_project_id ON variables(project_id);
CREATE INDEX idx_variables_function_name ON variables(function_name);
CREATE INDEX idx_variables_is_input ON variables(is_input);
CREATE INDEX idx_variables_input_variables ON variables USING GIN (input_variables);

-- Indexes for variable_values table
CREATE INDEX idx_variable_values_variable_id ON variable_values(variable_id);
CREATE INDEX idx_variable_values_entity_instance_id ON variable_values(entity_instance_id);
CREATE INDEX idx_variable_values_context ON variable_values USING GIN (context);
CREATE INDEX idx_variable_values_value ON variable_values USING GIN (value);

-- Indexes for projects table
CREATE INDEX idx_projects_name ON projects(name);