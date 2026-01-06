from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from oam import Property
from .entity import Entity

@dataclass
class EntityTag:
    id:         Optional[str]      = None
    entity:     Optional[Entity]   = None
    prop:       Optional[Property] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def ttype(self) -> Optional[str]:
        if not self.prop:
            return None
        return self.prop.property_type.value
    
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
            **_flatten(self.prop.to_dict())
        }
