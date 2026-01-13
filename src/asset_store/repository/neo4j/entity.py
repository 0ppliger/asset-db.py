from asset_store.types.entity import Entity
from asset_model import Asset
from asset_model import OAMObject
from asset_model import AssetType
from asset_model import get_asset_by_type
from asset_model import describe_type
from typing import Optional
from typing import cast
from datetime import datetime
from neo4j import Result
from neo4j.time import DateTime
from neo4j.graph import Node
from uuid import uuid4
from asset_store.events import events

def _node_to_entity(node: Node) -> Entity:
    id = node.get("entity_id")
    if id is None:
        raise Exception("Unable to extract 'entity_id'")

    _created_at = node.get("created_at")
    if not isinstance(_created_at, DateTime):
        raise Exception("Unable to extract 'created_at'")
    created_at = _created_at.to_native()

    _updated_at = node.get("updated_at")
    if not isinstance(_updated_at, DateTime):
        raise Exception("Unable to extract 'updated_at'")
    updated_at = _updated_at.to_native()

    _etype = node.get("etype")
    if _etype is None:
        raise Exception("Unable to extract 'etype'")

    asset_type = AssetType(_etype)

    try:
        asset_cls = get_asset_by_type(asset_type)
    except Exception as e:
        raise e

    props = describe_type(asset_cls)
    d = {}
    for prop_key in props:
        prop_value = node.get(prop_key)
        if prop_value is None:
            continue
        
        d[prop_key] = prop_value

    extra_props = list(filter(lambda e: e.startswith("extra_"), node.keys()))
    extra = { key: node.get(key) for key in extra_props }

    d.update(extra)
        
    asset = cast(Asset, OAMObject.from_dict(asset_cls, d))
    return Entity(
        id=id,
        created_at=created_at,
        updated_at=updated_at,
        asset=asset
    )

def _find_existing_entity(self, entity: Entity) -> Optional[Entity]:
    if entity.id != None and entity.id != "":
        return self.find_entity_by_id(entity.id)
    else:
        findings = self.find_entities_by_content(entity.asset)
        if len(findings) > 0:
            return findings[0]
    return None

def create_entity(self, entity: Entity) -> events.EntityInserted | events.EntityUpdated | events.EntityUntouched:
    new_entity: Optional[Entity] = None
    old_entity: Optional[Entity] = _find_existing_entity(self, entity)

    # If the entity does not exist, create it
    if old_entity is None:
        new_entity = Entity(
            id         = str(uuid4()),
            created_at = datetime.now(),
            updated_at = datetime.now(),
            asset      = entity.asset
        )

        try:
            record = self.db.execute_query(
                f"CREATE (a:Entity:{new_entity.etype} $props) RETURN a",
                {"props": new_entity.to_dict()},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

        if record is None:
            raise Exception("no records returned from the query")

        return events.EntityInserted(entity=new_entity)

    # If the entity already exists and has new data, update it
    if entity.asset.is_fresher_than(old_entity.asset):
        new_entity = Entity(
            id         = old_entity.id,
            asset      = old_entity.asset.override_with(entity.asset),
            created_at = old_entity.created_at,
            updated_at = datetime.now()
        )

        try:
            record = self.db.execute_query(
                f"MATCH (a:Entity:{new_entity.etype} {{entity_id: $id}}) SET a=$props RETURN a",
                {"id": new_entity.id, "props": new_entity.to_dict()},
                result_transformer_=Result.single)
        except Exception as e:
            raise e

        if record is None:
            raise Exception("no records returned from the query")

        return events.EntityUpdated(old_entity=old_entity, entity=new_entity)

    # If the entity already exists and has no new data, return the existing entity
    return events.EntityUntouched(entity=old_entity)

def create_asset(self, asset: Asset) -> events.EntityInserted | events.EntityUpdated | events.EntityUntouched:
    return self.create_entity(
        Entity(asset=asset))

def find_entity_by_id(self, id: str) -> Entity:
    try:
        record = self.db.execute_query(
            f"MATCH (a:Entity {{entity_id: $id}}) RETURN a",
            {"id": id},
            result_transformer_=Result.single)
    except Exception as e:
        raise e

    if record is None:
        raise Exception("the entity with ID {id} was not found")

    node = record.get("a")
    if node is None:
        raise Exception("the record value for the node is empty")

    entity = _node_to_entity(node)
    return entity

def find_entities_by_content(self, asset: Asset, since: Optional[datetime]) -> list[Entity]:
    entities: list[Entity] = []        

    props = asset.to_dict()
    props_filters = " AND ".join([f"a.{k} = ${k}" for k in props.keys()])
    
    query = f"MATCH (a:{asset.asset_type.value}) WHERE {props_filters} RETURN a"
    if since is not None:
        query = f"MATCH (a:{asset.asset_type.value}) WHERE {props_filters} AND a.updated_at >= localDateTime('{since.isoformat()}') RETURN a"

    try:
        records, summary, keys = self.db.execute_query(query, props)
    except Exception as e:
        raise e

    if len(records) == 0:
        return entities

    for rec in records:
        node = rec.get("a")
        if node is None:
            continue

        entities.append(_node_to_entity(node))

    return entities

def find_entities_by_type(self, atype: AssetType, since: Optional[datetime]) -> list[Entity]:
    query = f"MATCH (a:{atype.value} RETURN a)"
    if since is not None:
        query = f"MATCH (a:{atype.value}) WHERE a.updated_at >= localDateTime('{since.isoformat()}') RETURN a"

    try:
        records, summary, keys = self.db.execute_query(query)
    except Exception as e:
        raise e

    if len(records) == 0:
        raise Exception("no entities of the specified type")

    results: list[Entity] = []
    for record in records:
        node = record.get("a")
        if node is None:
            raise Exception("the record value for the node is empty")

        try:
            entity = _node_to_entity(node)
        except Exception as e:
            raise e

        results.append(entity)

    if len(results) == 0:
        raise Exception("no entities of the specified type")

    return results

def delete_entity(self, id: str) -> events.EntityDeleted:
    entity = self.find_entity_by_id(id)

    try:
        self.db.execute_query(
            "MATCH (n:Entity {entity_id: $id}) DETACH DELETE n",
            {"id": id})
    except Exception as e:
        raise e

    return events.EntityDeleted(old_entity=entity)