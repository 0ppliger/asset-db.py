from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from oam import Property
from .entity import Entity

@dataclass
class EntityTag:
    entity:     Entity
    property:   Property
    id:         Optional[str]      = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def ttype(self) -> str:
        return self.property.property_type.value
    
    def to_dict(self) -> dict:
        def _flatten(d) -> dict:
            flat = {}
            for k, v in d.items():
                if isinstance(v, dict):
                    flat.update(_flatten(v))
                else:
                    flat[k] = v
            return flat

        return {
            "tag_id":     self.id,
            "entity_id":  self.entity.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "ttype":      self.ttype,
            **_flatten(self.property.to_dict())
        }
