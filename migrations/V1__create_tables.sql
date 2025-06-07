CREATE SCHEMA IF NOT EXISTS variables_engine;

SET search_path TO variables_engine, public;

-- Projects table (must come first due to FK dependencies)
CREATE TABLE projects (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Entities table
CREATE TABLE entities (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    project_id VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT fk_entities_project FOREIGN KEY (project_id) REFERENCES projects(id)
);

-- Entity instances/values table
CREATE TABLE entity_values (
    id SERIAL PRIMARY KEY,
    entity_id VARCHAR(50) NOT NULL REFERENCES entities(id),
    instance_id VARCHAR(255) NOT NULL,
    metadata JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_entity_instance UNIQUE (entity_id, instance_id)
);

-- Variables table
CREATE TABLE variables (
    id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    entity_id VARCHAR(50) NOT NULL REFERENCES entities(id),
    is_input BOOLEAN NOT NULL DEFAULT true,
    is_persisted BOOLEAN DEFAULT false,
    function_name VARCHAR(255),
    metadata JSONB,
    description TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT check_function_name_if_not_input CHECK (
        (is_input = true) OR (is_input = false AND function_name IS NOT NULL)
    )
);

-- Variable values table
CREATE TABLE variable_values (
    id SERIAL PRIMARY KEY,
    variable_id VARCHAR(50) NOT NULL REFERENCES variables(id),
    entity_instance_id VARCHAR(255) NOT NULL,
    context JSONB,
    value JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    
    CONSTRAINT unique_variable_instance UNIQUE (variable_id, entity_instance_id)
);

-- Add function to verify entity instance exists before inserting variable value
CREATE OR REPLACE FUNCTION check_entity_instance()
RETURNS TRIGGER AS $$
DECLARE
    entity_id_val VARCHAR(50);
BEGIN
    -- Get the entity_id for the variable
    SELECT entity_id INTO entity_id_val 
    FROM variables 
    WHERE id = NEW.variable_id;
    
    -- Check if the entity instance exists
    IF NOT EXISTS (
        SELECT 1 
        FROM entity_values 
        WHERE entity_id = entity_id_val 
        AND instance_id = NEW.entity_instance_id
    ) THEN
        RAISE EXCEPTION 'Entity instance does not exist: entity_id=%, instance_id=%', 
                        entity_id_val, NEW.entity_instance_id;
    END IF;
    
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Add trigger to verify entity instance exists
CREATE TRIGGER check_entity_instance_trigger
BEFORE INSERT OR UPDATE ON variable_values
FOR EACH ROW EXECUTE FUNCTION check_entity_instance();