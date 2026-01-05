from neo4j import GraphDatabase
from neo4j import Driver
from neo4j import Result
from typing import Optional
from datetime import datetime
from uuid import uuid4
from oam import Asset
from oam import valid_relationship
from asset_db.types.entity import Entity
from asset_db.types.edge import Edge
from asset_db.repository.repository import Repository
from asset_db.repository.neo4j.extract import node_to_entity

#class NeoRepository(Repository):
class NeoRepository():
    db:     Driver
    dbname: str

    def __init__(self,  uri: str, auth: tuple[str, str]):
        self._uri = uri
        self._auth = auth
    
    def __enter__(self):
        _db = GraphDatabase.driver(self._uri, auth=self._auth)
        _db.verify_connectivity()
        
        self.db = _db
        return self

    def __exit__(self, exc_type, exc, tb):
        self.db.close()

    def create_entity(self, entity: Entity) -> Entity:
        _entity = Entity(
            id=entity.id,
            asset=entity.asset,
            created_at=entity.created_at,
            updated_at=entity.updated_at)
        
        if not _entity.id:
            _entity.id = str(uuid4())

        if not _entity.created_at:
            _entity.created_at = datetime.now()

        if not _entity.updated_at:
            _entity.updated_at = datetime.now()

        record = self.db.execute_query(
            f"CREATE (a:Entity:{_entity.etype} $props) RETURN a",
            {"props": _entity.to_dict()},
            result_transformer_=Result.single)

        return _entity

    def find_entity_by_id(self, id: str) -> Optional[Entity]:
        record = self.db.execute_query(
            f"MATCH (a:Entity {{entity_id: $id}}) RETURN a",
            {"id": id},
            result_transformer_=Result.single)

        if record is None:
            return None

        node = record.get("a")
        if node is None:
            raise Exception("Unable to fetch node from record.")

        entity = node_to_entity(node)
        return entity
    
    def create_asset(self, asset: Asset) -> Entity:
        return self.create_entity(
            Entity(asset=asset))

    def create_edge(self, edge: Edge) -> Edge:
        _edge = Edge(
            edge.relation,
            edge.from_entity,
            edge.to_entity)

        if not valid_relationship(
                _edge.from_entity.asset.asset_type,
                _edge.relation.label,
                _edge.relation.relation_type,
                _edge.to_entity.asset.asset_type
        ):
            raise Exception("Invalid relationship")

        if not _edge.id:
            _edge.id = str(uuid4())

        if not _edge.created_at:
            _edge.created_at = datetime.now()

        if not _edge.updated_at:
            _edge.updated_at = datetime.now()

        record = self.db.execute_query(
            f"""
            MATCH (from:Entity {{entity_id: "{_edge.from_entity.id}"}})
            MATCH (to:Entity {{entity_id: "{_edge.to_entity.id}"}})
            CREATE (from) -[r:{_edge.label} $props]-> (to) RETURN r
            """,
            {"props": _edge.to_dict()},
            result_transformer_=Result.single)

        return _edge
    
        
 
