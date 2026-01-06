from oam import Asset, AssetType
from oam import Property
from oam import DNSRecordProperty
from oam import SimpleProperty
from oam import SourceProperty
from oam import VulnProperty
from typing import Tuple, Optional

# TODO
def query_node_by_asset_key(varname: str, asset: Asset) -> str:
    match(asset.asset_type):
        case AssetType.FQDN:
            return f'({varname}:{asset.asset_type.value} {{name: "{asset.name}"}})'
        case AssetType.IPAddress:
            return f'({varname}:{asset.asset_type.value} {{address: "{asset.address}"}})'
        case _:
            raise Exception("Unsupported asset type")

def query_node_by_property_key_value(varname: str, label: str, prop: Property) -> str:
    node = ""
    if isinstance(prop, DNSRecordProperty):
        node = f"({varname}:{label} {{property_name: '{prop.property_name}', data: '{prop.data}'}})"
    elif isinstance(prop, SimpleProperty):
        node = f"({varname}:{label} {{property_name: '{prop.property_name}', property_value: '{prop.property_value}'}})"
    elif isinstance(prop, SourceProperty):
        node = f"({varname}:{label} {{name: '{prop.source}', confidence: {prop.confidence}}})"
    elif isinstance(prop, VulnProperty):
        node = f"({varname}:{label} {{vuln_id: '{prop.id}', desc: '{prop.description}'}})"
    
    if not node:
        raise Exception("asset type not supported")

    return node
