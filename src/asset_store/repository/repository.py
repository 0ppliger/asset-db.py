from abc import ABC
from abc import abstractmethod
from typing import Optional
from asset_model import Asset
from asset_model import AssetType
from asset_model import Property
from asset_model import Relation
from asset_store.events import events 
from asset_store.types.entity import Entity
from asset_store.types.entity_tag import EntityTag
from asset_store.types.edge import Edge
from asset_store.types.edge_tag import EdgeTag
from datetime import datetime

class Repository(ABC):
    
    @abstractmethod
    def get_db_type(self) -> str:
        pass
    
    @abstractmethod
    def create_entity(
        self, entity: Entity
    ) -> events.EntityInserted | events.EntityUpdated | events.EntityUntouched:
        pass
    
    @abstractmethod
    def create_asset(
        self, asset: Asset
    ) -> events.EntityInserted | events.EntityUpdated | events.EntityUntouched:
        pass
    
    @abstractmethod
    def find_entity_by_id(
        self, id: str
    ) -> Entity:
        pass
    
    @abstractmethod
    def find_entities_by_content(
        self, asset: Asset, since: Optional[datetime]
    ) -> list[Entity]:
        pass
    
    @abstractmethod
    def find_entities_by_type(
        self, atype: AssetType, since: Optional[datetime]
    ) -> list[Entity]:
        pass
    
    @abstractmethod
    def delete_entity(
        self, id: str
    ) -> events.EntityDeleted:
        pass

    @abstractmethod
    def create_relation(
        self, relation: Relation, from_entity: Entity, to_entity: Entity
    ) -> events.EdgeInserted | events.EdgeUpdated | events.EdgeUntouched:
        pass
    
    @abstractmethod
    def create_edge(
        self, edge: Edge
    ) -> events.EdgeInserted | events.EdgeUpdated | events.EdgeUntouched:
        pass
    
    @abstractmethod
    def find_edge_by_id(
        self, id: str
    ) -> Edge:
        pass
    
    @abstractmethod
    def incoming_edges(
        self, entity: Entity, since: Optional[datetime], *args: str
    ) -> list[Edge]:
        pass
    
    @abstractmethod
    def outgoing_edges(
        self, entity: Entity, since: Optional[datetime], *args: str
    ) -> list[Edge]:
        pass
    
    @abstractmethod
    def delete_edge(
        self, id: str
    ) -> events.EdgeDeleted:
        pass
    
    @abstractmethod
    def create_entity_tag(
        self, tag: EntityTag
    ) -> events.EntityTagInserted | events.EntityTagUpdated | events.EntityTagUntouched:
        pass
    
    @abstractmethod
    def create_entity_property(
        self, entity: Entity, prop: Property
    ) -> events.EntityTagInserted | events.EntityTagUpdated | events.EntityTagUntouched:
        pass
    
    @abstractmethod
    def find_entity_tag_by_id(
        self, id: str
    ) -> EntityTag:
        pass
    
    @abstractmethod
    def find_entity_tags_by_content(
        self, prop: Property, since: Optional[datetime]
    ) -> list[EntityTag]:
        pass
    
    @abstractmethod
    def find_entity_tags(
        self, entity: Entity, since: Optional[datetime], *args: str
    ) -> list[EntityTag]:
        pass
    
    @abstractmethod
    def delete_entity_tag(
        self, id: str
    ) -> events.EntityTagDeleted:
        pass
    
    @abstractmethod
    def create_edge_tag(
        self, tag: EdgeTag
    ) -> events.EdgeTagInserted | events.EdgeTagUpdated | events.EdgeTagUntouched:
        pass
    
    @abstractmethod
    def create_edge_property(
        self, edge: Edge, prop: Property
    ) -> events.EdgeTagInserted | events.EdgeTagUpdated | events.EdgeTagUntouched:
        pass
    
    @abstractmethod
    def find_edge_tag_by_id(
        self, id: str
    ) -> EdgeTag:
        pass
    
    @abstractmethod
    def find_edge_tags_by_content(
        self, prop: Property, since: Optional[datetime]
    ) -> list[EdgeTag]:
        pass
    
    @abstractmethod
    def find_edge_tags(
        self, edge: Edge, since: Optional[datetime], *args: str
    ) -> list[EdgeTag]:
        pass
    
    @abstractmethod
    def delete_edge_tag(
        self, id: str
    ) -> events.EdgeTagDeleted:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

