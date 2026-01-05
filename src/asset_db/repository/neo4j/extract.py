from neo4j.graph import Node
from neo4j.time import DateTime
from oam import AssetType
from oam import FQDN
from asset_db.types.entity import Entity

def node_to_entity(node: Node) -> Entity:
    entity = Entity()

    entity.id = node.get("entity_id")
    if entity.id is None:
        raise Exception("Unable to extract 'entity_id'")

    _created_at = node.get("created_at")
    if not isinstance(_created_at, DateTime):
        raise Exception("Unable to extract 'created_at'")
    entity.created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if not isinstance(_updated_at, DateTime):
        raise Exception("Unable to extract 'updated_at'")
    entity.updated_at = _updated_at.to_native()

    _etype = node.get("etype")
    if _etype is None:
        raise Exception("Unable to extract 'etype'")

    _asset_type = AssetType(_etype)

    match(_asset_type):
        case AssetType.FQDN:
            _fqdn_name = node.get("name")
            if _fqdn_name is None:
                raise Exception("Unable to extract 'name'")
            entity.asset = FQDN(_fqdn_name)
            pass
        case _:
            raise Exception("Unsupported entity")
    
    return entity
