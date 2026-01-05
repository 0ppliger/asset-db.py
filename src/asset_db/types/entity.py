from dataclasses import dataclass
from datetime import datetime
from oam import Asset
from typing import Optional

@dataclass
class Entity:
    id:         Optional[str]      = None
    asset:      Optional[Asset]    = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    @property
    def etype(self) -> Optional[str]:
        if not self.asset:
            return None
        return self.asset.asset_type.value
    
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
            "entity_id":  self.id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "etype":      self.etype,
            **_flatten(self.asset.to_dict())
        }

