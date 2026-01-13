from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from asset_store.types import Entity, Edge, EdgeTag, EntityTag
from enum import Enum

class EventType(str, Enum):
    EntityInserted = "EntityInserted"
    EntityUpdated = "EntityUpdated"
    EntityUntouched = "EntityUntouched"
    EntityDeleted = "EntityDeleted"
    EdgeInserted = "EdgeInserted"
    EdgeUpdated = "EdgeUpdated"
    EdgeUntouched = "EdgeUntouched"
    EdgeDeleted = "EdgeDeleted"
    EntityTagInserted = "EntityTagInserted"
    EntityTagUpdated = "EntityTagUpdated"
    EntityTagUntouched = "EntityTagUntouched"
    EntityTagDeleted = "EntityTagDeleted"
    EdgeTagInserted = "EdgeTagInserted"
    EdgeTagUpdated = "EdgeTagUpdated"
    EdgeTagUntouched = "EdgeTagUntouched"
    EdgeTagDeleted = "EdgeTagDeleted"
    
@dataclass
class Event(ABC):
    emitted_at: datetime = field(init=False, default_factory=datetime.now)

    @property
    @abstractmethod
    def event_type(self) -> EventType:
        pass

@dataclass
class EntityInserted(Event):
    entity: Entity

    @property
    def event_type(self) -> EventType:
        return EventType.EntityInserted

@dataclass
class EntityUpdated(Event):
    old_entity: Entity
    entity: Entity

    @property
    def event_type(self) -> EventType:
        return EventType.EntityUpdated
    
@dataclass
class EntityUntouched(Event):
    entity: Entity

    @property
    def event_type(self) -> EventType:
        return EventType.EntityUntouched
    
@dataclass
class EntityDeleted(Event):
    old_entity: Entity

    @property
    def event_type(self) -> EventType:
        return EventType.EntityDeleted

@dataclass
class EdgeInserted(Event):
    edge: Edge

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeInserted

@dataclass
class EdgeUpdated(Event):
    old_edge: Edge
    edge: Edge

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeUpdated

@dataclass
class EdgeUntouched(Event):
    edge: Edge

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeUntouched
    
@dataclass
class EdgeDeleted(Event):
    old_edge: Edge

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeDeleted

@dataclass
class EntityTagInserted(Event):
    tag: EntityTag

    @property
    def event_type(self) -> EventType:
        return EventType.EntityTagInserted

@dataclass
class EntityTagUpdated(Event):
    old_tag: EntityTag
    tag: EntityTag

    @property
    def event_type(self) -> EventType:
        return EventType.EntityTagUpdated

@dataclass
class EntityTagUntouched(Event):
    tag: EntityTag

    @property
    def event_type(self) -> EventType:
        return EventType.EntityTagUntouched
    
@dataclass
class EntityTagDeleted(Event):
    old_tag: EntityTag

    @property
    def event_type(self) -> EventType:
        return EventType.EntityTagDeleted

@dataclass
class EdgeTagInserted(Event):
    tag: EdgeTag

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeTagInserted

@dataclass
class EdgeTagUpdated(Event):
    old_tag: EdgeTag
    tag: EdgeTag

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeTagUpdated

@dataclass
class EdgeTagUntouched(Event):
    tag: EdgeTag

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeTagUntouched
    
    
@dataclass
class EdgeTagDeleted(Event):
    old_tag: EdgeTag

    @property
    def event_type(self) -> EventType:
        return EventType.EdgeTagDeleted

