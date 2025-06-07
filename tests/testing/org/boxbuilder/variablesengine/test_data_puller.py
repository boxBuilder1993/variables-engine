import pytest
from unittest.mock import Mock, AsyncMock
from typing import Dict, Any

from org.boxbuilder.variablesengine.models.project import Project
from org.boxbuilder.variablesengine.models.entity import Entity
from org.boxbuilder.variablesengine.models.variable import Variable
from org.boxbuilder.variablesengine.data_puller import DataPuller
from org.boxbuilder.database.postgres.query_helper import QueryHelper


class TestDataPuller:
    @pytest.fixture
    def sample_project(self):
        """Create a sample project with entities and variables for testing."""
        # Create test entities
        entities = [
            Entity(id="entity1", name="Customer"),
            Entity(id="entity2", name="Order")
        ]
        
        # Create test variables
        variables = [
            # Customer variables
            Variable(
                id="var1",
                name="id",
                entity_id="entity1",
                is_input=True,
                function_name=None,
                metadata={}
            ),
            Variable(
                id="var2",
                name="name",
                entity_id="entity1",
                is_input=True,
                function_name=None,
                metadata={}
            ),
            Variable(
                id="var3",
                name="credit_score",
                entity_id="entity1",
                is_input=False,
                function_name="calculate_credit",
                metadata={"input_variables": ["age", "income"]}
            ),
            Variable(
                id="var7",
                name="age",
                entity_id="entity1",
                is_input=True,
                function_name=None,
                metadata={}
            ),
            Variable(
                id="var8",
                name="income",
                entity_id="entity1",
                is_input=True,
                function_name=None,
                metadata={}
            ),
            # Order variables
            Variable(
                id="var4",
                name="id",
                entity_id="entity2",
                is_input=True,
                function_name=None,
                metadata={}
            ),
            Variable(
                id="var5",
                name="customer_id",
                entity_id="entity2",
                is_input=True,
                function_name=None,
                metadata={"foreign_key": {"entity": "Customer"}}
            ),
            Variable(
                id="var6",
                name="amount",
                entity_id="entity2",
                is_input=True,
                function_name=None,
                metadata={}
            )
        ]
        
        return Project(id="project1", name="Test Project", entities=entities, variables=variables)
    
    @pytest.fixture
    def mock_query_helper(self):
        """Create a mock QueryHelper for testing."""
        helper = Mock(spec=QueryHelper)
        helper.get_query_results_as_dictionaries = AsyncMock()
        return helper
    
    @pytest.fixture
    def data_puller(self, mock_query_helper):
        """Create a DataPuller instance with the mock QueryHelper."""
        return DataPuller(mock_query_helper)
    
    def test_build_dependency_graph(self, data_puller, sample_project):
        """Test building the dependency graph."""
        graph = data_puller.build_dependency_graph(sample_project)
        
        # Print graph contents for debugging
        print("\nGraph contents:")
        for var_name, dep in graph.items():
            print(f"{var_name}: {dep}")
        
        # Test basic structure
        assert len(graph) == 7  # All variables should be in the graph
        
        # Test credit_score dependencies
        credit_score = graph["credit_score"]
        assert credit_score.dependencies == {"age", "income"}
        assert credit_score.foreign_keys == {}
        
        # Test customer_id foreign key
        customer_id = graph["customer_id"]
        assert customer_id.dependencies == set()
        assert customer_id.foreign_keys == {"customer_id": "Customer"}
    
    def test_get_required_variables(self, data_puller, sample_project):
        """Test determining required variables."""
        requested_outputs = {
            "Customer": ["credit_score"],
            "Order": ["amount"]
        }
        provided_inputs = {
            "Customer": {"id": "123"}
        }
        
        required_vars = data_puller.get_required_variables(
            sample_project,
            requested_outputs,
            provided_inputs
        )
        
        # Test Customer variables
        assert "Customer" in required_vars
        assert required_vars["Customer"] == {"credit_score", "age", "income", "id"}
        
        # Test Order variables
        assert "Order" in required_vars
        assert required_vars["Order"] == {"amount"}
    
    def test_get_required_variables_with_foreign_keys(self, data_puller, sample_project):
        """Test required variables when foreign keys are involved."""
        requested_outputs = {
            "Order": ["amount"]
        }
        provided_inputs = {
            "Order": {"customer_id": "123"}
        }
        
        required_vars = data_puller.get_required_variables(
            sample_project,
            requested_outputs,
            provided_inputs
        )
        
        # Test Order variables
        assert "Order" in required_vars
        assert required_vars["Order"] == {"amount", "customer_id"}
        
        # Test Customer variables (due to foreign key)
        assert "Customer" in required_vars
        assert required_vars["Customer"] == {"id"}
    
    @pytest.mark.asyncio
    async def test_pull_data(self, data_puller, sample_project, mock_query_helper):
        """Test pulling data from the database."""
        # Mock database response
        mock_query_helper.get_query_results_as_dictionaries.return_value = [
            {
                "entity_name": "Customer",
                "entity_instance_id": "123",
                "variable_name": "name",
                "value": "John Doe"
            },
            {
                "entity_name": "Customer",
                "entity_instance_id": "123",
                "variable_name": "credit_score",
                "value": 750
            },
            {
                "entity_name": "Order",
                "entity_instance_id": "456",
                "variable_name": "amount",
                "value": 100.50
            }
        ]
        
        requested_outputs = {
            "Customer": ["name", "credit_score"],
            "Order": ["amount"]
        }
        provided_inputs = {
            "Customer": {"id": "123"}
        }
        
        results = await data_puller.pull_data(
            sample_project,
            requested_outputs,
            provided_inputs
        )
        
        # Test results structure
        assert "Customer" in results
        assert "Order" in results
        
        # Test Customer data
        assert results["Customer"]["name"] == "John Doe"
        assert results["Customer"]["credit_score"] == 750
        
        # Test Order data
        assert results["Order"]["amount"] == 100.50
        
        # Verify query was called with correct parameters
        mock_query_helper.get_query_results_as_dictionaries.assert_called_once()
        query = mock_query_helper.get_query_results_as_dictionaries.call_args[1]["query"]
        
        # Verify query contains expected elements
        assert "WITH" in query
        assert "customer_data" in query.lower()
        assert "order_data" in query.lower()
        assert "UNION ALL" in query
    
    @pytest.mark.asyncio
    async def test_pull_data_error_handling(self, data_puller, sample_project, mock_query_helper):
        """Test error handling during data pulling."""
        # Mock database error
        mock_query_helper.get_query_results_as_dictionaries.side_effect = Exception("Database error")
        
        results = await data_puller.pull_data(
            sample_project,
            {"Customer": ["name"]},
            {}
        )
        
        # Should return empty dict on error
        assert results == {} 