"""
Factory classes for creating FHIR resources
"""
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any, List
import json

from .base import FHIRResource
from .organization import FHIROrganization
from .endpoint import FHIREndpoint


class ResourceFactory(ABC):
    """Abstract factory for creating FHIR resources"""
    
    @abstractmethod
    def create_resource(self, *, json_data: Dict[str, Any], vendor_name: str) -> Optional[FHIRResource]:
        """Create a FHIR resource from JSON data"""
        pass
    
    @staticmethod
    def _load_json_from_file(file_path: str) -> Optional[Dict[str, Any]]:
        """Load JSON data from file with error handling"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
            print(f"Error loading JSON from {file_path}: {e}")
            return None
    
    def create_resource_from_file(self, *, file_path: str, vendor_name: str) -> Optional[FHIRResource]:
        """Create a FHIR resource from a JSON file"""
        json_data = self._load_json_from_file(file_path)
        if json_data is None:
            return None
        
        return self.create_resource(json_data=json_data, vendor_name=vendor_name)


class FHIROrganizationFactory(ResourceFactory):
    """Factory for creating FHIR Organization resources"""
    
    def create_resource(self, *, json_data: Dict[str, Any], vendor_name: str) -> Optional[FHIROrganization]:
        """Create a FHIR Organization from JSON data"""
        # Validate that this is organization data
        resource = json_data.get('resource', {})
        if resource.get('resourceType') != 'Organization':
            return None
        
        # Must have fullUrl for our processing approach
        full_url = json_data.get('fullUrl', '')
        if not full_url:
            # Skip resources without fullUrl as they can't be properly referenced
            return None
        
        try:
            return FHIROrganization(
                full_url=full_url,
                resource_data=resource,
                vendor_name=vendor_name
            )
        except Exception as e:
            print(f"Error creating FHIROrganization: {e}")
            return None
    
    @staticmethod
    def can_handle_resource(json_data: Dict[str, Any]) -> bool:
        """Check if this factory can handle the given JSON data"""
        resource = json_data.get('resource', {})
        return (
            resource.get('resourceType') == 'Organization' and
            'fullUrl' in json_data
        )


class FHIREndpointFactory(ResourceFactory):
    """Factory for creating FHIR Endpoint resources"""
    
    def create_resource(self, *, json_data: Dict[str, Any], vendor_name: str) -> Optional[FHIREndpoint]:
        """Create a FHIR Endpoint from JSON data"""
        # Validate that this is endpoint data
        resource = json_data.get('resource', {})
        if resource.get('resourceType') != 'Endpoint':
            return None
        
        # Must have fullUrl for our processing approach
        full_url = json_data.get('fullUrl', '')
        if not full_url:
            # Skip resources without fullUrl
            return None
        
        try:
            return FHIREndpoint(
                full_url=full_url,
                resource_data=resource,
                vendor_name=vendor_name
            )
        except Exception as e:
            print(f"Error creating FHIREndpoint: {e}")
            return None
    
    @staticmethod
    def can_handle_resource(json_data: Dict[str, Any]) -> bool:
        """Check if this factory can handle the given JSON data"""
        resource = json_data.get('resource', {})
        return (
            resource.get('resourceType') == 'Endpoint' and
            'fullUrl' in json_data
        )


class ResourceFactoryRegistry:
    """Registry for managing multiple resource factories"""
    
    def __init__(self):
        self.factories = []
        
        # Register default factories
        self.register_factory(FHIROrganizationFactory())
        self.register_factory(FHIREndpointFactory())
    
    def register_factory(self, factory: ResourceFactory):
        """Register a new resource factory"""
        self.factories.append(factory)
    
    def create_resource(self, *, json_data: Dict[str, Any], vendor_name: str) -> Optional[FHIRResource]:
        """Try to create a resource using the appropriate factory"""
        for factory in self.factories:
            # Check if factory can handle this resource type
            if hasattr(factory, 'can_handle_resource'):
                if factory.can_handle_resource(json_data):
                    resource = factory.create_resource(
                        json_data=json_data, 
                        vendor_name=vendor_name
                    )
                    if resource is not None:
                        return resource
            else:
                # Fallback: try each factory until one succeeds
                resource = factory.create_resource(
                    json_data=json_data, 
                    vendor_name=vendor_name
                )
                if resource is not None:
                    return resource
        
        # No factory could handle this resource
        return None
    
    def create_resource_from_file(self, *, file_path: str, vendor_name: str) -> Optional[FHIRResource]:
        """Create a resource from a JSON file using the appropriate factory"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                json_data = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError, IOError) as e:
            print(f"Error loading JSON from {file_path}: {e}")
            return None
        
        return self.create_resource(json_data=json_data, vendor_name=vendor_name)
    
    def get_supported_resource_types(self) -> List[str]:
        """Get list of supported resource types"""
        supported_types = []
        for factory in self.factories:
            if isinstance(factory, FHIROrganizationFactory):
                supported_types.append('Organization')
            elif isinstance(factory, FHIREndpointFactory):
                supported_types.append('Endpoint')
        return supported_types


# Default factory registry instance
default_factory_registry = ResourceFactoryRegistry()


def create_fhir_resource_from_file(*, file_path: str, vendor_name: str) -> Optional[FHIRResource]:
    """Convenience function to create a FHIR resource from file using default registry"""
    return default_factory_registry.create_resource_from_file(
        file_path=file_path,
        vendor_name=vendor_name
    )


def create_fhir_resource_from_json(*, json_data: Dict[str, Any], vendor_name: str) -> Optional[FHIRResource]:
    """Convenience function to create a FHIR resource from JSON using default registry"""
    return default_factory_registry.create_resource(
        json_data=json_data,
        vendor_name=vendor_name
    )
