import uuid

# UUIDv5 Namespaces
NAMESPACE_SALE = uuid.UUID("10000000-0000-0000-0000-000000000000")
NAMESPACE_LINEITEM = uuid.UUID("20000000-0000-0000-0000-000000000000")
NAMESPACE_ADDRESS = uuid.UUID("30000000-0000-0000-0000-000000000000")
NAMESPACE_ATTRIBUTE = uuid.UUID("40000000-0000-0000-0000-000000000000")

def generate_deterministic_id(namespace: uuid.UUID, parts: list) -> str:
    key = '|'.join(str(p).strip().lower() for p in parts if p is not None)
    return str(uuid.uuid5(namespace, key))
