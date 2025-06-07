-- Set the search path for convenience
SET search_path TO variables_engine, public;

-- Function to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add triggers for all tables with updated_at column
CREATE TRIGGER update_entities_updated_at
BEFORE UPDATE ON entities
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_projects_updated_at
BEFORE UPDATE ON projects
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_variables_updated_at
BEFORE UPDATE ON variables
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_variable_values_updated_at
BEFORE UPDATE ON variable_values
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Function to get variable value with context matching
CREATE OR REPLACE FUNCTION get_variable_value(variable_id VARCHAR(50), entity_instance_id VARCHAR(255), context JSONB DEFAULT NULL)
RETURNS JSONB AS $$
BEGIN
    RETURN (
        SELECT value
        FROM variable_values
        WHERE variable_id = get_variable_value.variable_id
        AND entity_instance_id = get_variable_value.entity_instance_id
        AND (get_variable_value.context IS NULL OR context @> get_variable_value.context)
        ORDER BY created_at DESC
        LIMIT 1
    );
END;
$$ LANGUAGE plpgsql;

-- Function to upsert variable value
CREATE OR REPLACE FUNCTION upsert_variable_value(
    p_variable_id VARCHAR(50),
    p_entity_instance_id VARCHAR(255),
    p_value JSONB,
    p_context JSONB DEFAULT NULL
)
RETURNS VOID AS $$
BEGIN
    INSERT INTO variable_values (variable_id, entity_instance_id, value, context)
    VALUES (p_variable_id, p_entity_instance_id, p_value, p_context)
    ON CONFLICT (variable_id, entity_instance_id, context)
    DO UPDATE SET
        value = p_value,
        updated_at = CURRENT_TIMESTAMP;
END;
$$ LANGUAGE plpgsql;