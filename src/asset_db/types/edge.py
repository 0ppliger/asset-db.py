from typing import Optional
from dataclasses import dataclass
from datetime import datetime
from oam import Relation
from .entity import Entity

@dataclass
class Edge:
    id:          Optional[str]      = None
    relation:    Optional[Relation] = None
    from_entity: Optional[Entity]   = None
    to_entity:   Optional[Entity]   = None
    created_at:  Optional[datetime] = None
    updated_at:  Optional[datetime] = None

    @property
    def etype(self) -> Optional[str]:
        if not self.relation:
            return None
        return self.relation.relation_type.value

    @property
    def label(self) -> str:
        if not self.relation:
            return ""
        return self.relation.label.upper()
    
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
            "edge_id":    self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "etype":      self.etype,
            **_flatten(self.relation.to_dict())
        }
