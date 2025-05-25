import pytest

from org.boxbuilder.variablesengine.engine import Engine
from org.boxbuilder.variablesengine.models.project import Project
from org.boxbuilder.variablesengine.models.entity import Entity
from org.boxbuilder.variablesengine.models.variable import Variable


class TestEngine:
    @pytest.fixture
    def sample_project(self):
        """Create a sample project with entities and variables for testing."""
        entity1 = Entity(id="entity1", name="Customer")
        entity2 = Entity(id="entity2", name="Order")
        
        variables = [
            # Input variables
            Variable(
                id="var1", 
                name="age", 
                entity_id="entity1", 
                is_input=True,
                function_name=None,
                input_variables=[]
            ),
            Variable(
                id="var2", 
                name="income", 
                entity_id="entity1", 
                is_input=True,
                function_name=None,
                input_variables=[]
            ),
            # Derived variables
            Variable(
                id="var3", 
                name="is_adult", 
                entity_id="entity1", 
                is_input=False,
                function_name="check_adult",
                input_variables=["age"]
            ),
            Variable(
                id="var4", 
                name="credit_score", 
                entity_id="entity1", 
                is_input=False,
                function_name="calculate_credit_score",
                input_variables=["age", "income"]
            ),
            # Order entity variables
            Variable(
                id="var5", 
                name="amount", 
                entity_id="entity2", 
                is_input=True,
                function_name=None,
                input_variables=[]
            ),
            Variable(
                id="var6", 
                name="tax", 
                entity_id="entity2", 
                is_input=False,
                function_name="calculate_tax",
                input_variables=["amount"]
            ),
        ]
        
        return Project(id="project1", name="Test Project", entities=[entity1, entity2], variables=variables)
    
    @pytest.fixture
    def engine_with_functions(self):
        """Create an engine with mock functions in registry."""
        engine = Engine()
        
        # Add mock functions to registry
        engine.function_registry = {
            "check_adult": lambda age: age >= 18 if age is not None else None,
            "calculate_credit_score": lambda age, income: (age * 10 + income / 1000) if age is not None and income is not None else None,
            "calculate_tax": lambda amount: amount * 0.1 if amount is not None else None
        }
        
        return engine
    
    def test_execute_with_valid_inputs(self, sample_project, engine_with_functions):
        # Test inputs
        inputs = {
            "Customer": {
                "age": 30,
                "income": 50000
            },
            "Order": {
                "amount": 1000
            }
        }
        
        # Test outputs request
        outputs = {
            "Customer": ["is_adult", "credit_score"],
            "Order": ["tax"]
        }
        
        # Execute
        results = engine_with_functions.execute(sample_project, inputs, outputs)
        
        # Verify results
        assert "Customer" in results
        assert "Order" in results
        assert results["Customer"]["is_adult"] == True
        assert results["Customer"]["credit_score"] == 30 * 10 + 50000 / 1000  # 350.0
        assert results["Order"]["tax"] == 100.0
    
    def test_execute_with_missing_inputs(self, sample_project, engine_with_functions):
        # Test with missing inputs
        inputs = {
            "Customer": {
                "age": 30
                # Missing income
            }
        }
        
        outputs = {
            "Customer": ["is_adult", "credit_score"]
        }
        
        results = engine_with_functions.execute(sample_project, inputs, outputs)
        
        # Verify results - is_adult should work, credit_score should be None
        assert results["Customer"]["is_adult"] == True
        assert results["Customer"]["credit_score"] is None
    
    def test_execute_with_nonexistent_entity(self, sample_project, engine_with_functions):
        inputs = {"Customer": {"age": 30, "income": 50000}}
        outputs = {"NonExistentEntity": ["some_variable"]}
        
        results = engine_with_functions.execute(sample_project, inputs, outputs)
        
        # Verify no error raised and no results for nonexistent entity
        assert "NonExistentEntity" not in results
    
    def test_execute_with_nonexistent_variable(self, sample_project, engine_with_functions):
        inputs = {"Customer": {"age": 30, "income": 50000}}
        outputs = {"Customer": ["non_existent_var"]}
        
        results = engine_with_functions.execute(sample_project, inputs, outputs)
        
        # Verify no error raised and no results for nonexistent variable
        assert "Customer" in results
        assert "non_existent_var" not in results["Customer"]
    
    def test_variable_from_wrong_entity(self, sample_project, engine_with_functions):
        inputs = {"Customer": {"age": 30, "income": 50000}}
        # Try to request an Order variable for Customer entity
        outputs = {"Customer": ["tax"]}
        
        results = engine_with_functions.execute(sample_project, inputs, outputs)
        
        # Verify no error raised and no results for variable from wrong entity
        assert "Customer" in results
        assert "tax" not in results["Customer"]
    
    def test_missing_function(self, sample_project):
        # Create engine with empty function registry
        engine = Engine()
        engine.function_registry = {}
        
        inputs = {"Customer": {"age": 30}}
        outputs = {"Customer": ["is_adult"]}
        
        results = engine.execute(sample_project, inputs, outputs)
        
        # Verify variable with missing function returns None
        assert results["Customer"]["is_adult"] is None
    
    def test_function_exception(self, sample_project):
        # Create engine with function that raises exception
        engine = Engine()
        engine.function_registry = {
            "check_adult": lambda age: 1/0  # Will raise ZeroDivisionError
        }
        
        inputs = {"Customer": {"age": 30}}
        outputs = {"Customer": ["is_adult"]}
        
        results = engine.execute(sample_project, inputs, outputs)
        
        # Verify exception handling returns None
        assert results["Customer"]["is_adult"] is None
    
    def test_dependency_chain(self, sample_project):
        # Create a project with a chain of dependent variables
        entity = Entity(id="entity1", name="Test")
        variables = [
            Variable(id="var1", name="input1", entity_id="entity1", is_input=True, function_name=None, input_variables=[]),
            Variable(id="var2", name="level1", entity_id="entity1", is_input=False, function_name="add_one", input_variables=["input1"]),
            Variable(id="var3", name="level2", entity_id="entity1", is_input=False, function_name="add_one", input_variables=["level1"]),
            Variable(id="var4", name="level3", entity_id="entity1", is_input=False, function_name="add_one", input_variables=["level2"]),
        ]
        
        project = Project(id="project1", name="Test", entities=[entity], variables=variables)
        
        # Create engine with simple add_one function
        engine = Engine()
        engine.function_registry = {
            "add_one": lambda **kwargs: list(kwargs.values())[0] + 1 if list(kwargs.values())[0] is not None else None
        }
        
        inputs = {"Test": {"input1": 1}}
        outputs = {"Test": ["level3"]}
        
        results = engine.execute(project, inputs, outputs)
        
        # Verify dependency chain resolved correctly: 1 -> 2 -> 3 -> 4
        assert results["Test"]["level3"] == 4