# tests/test_api.py
import pytest
from app import app
import json

# List your tables and an example payload for POST/PUT
TABLES = {
    "distribution_centers": {
        "example": {"name": "Test DC", "latitude": 0.0, "longitude": 0.0},
        "pk": "id"
    },
    "products": {
        "example": {
            "cost": 1.23,
            "category": "TestCat",
            "name": "Test Product",
            "brand": "TestBrand",
            "retail_price": 4.56,
            "department": "TestDept",
            "sku": "SKU123",
            "distribution_center_id": 1
        },
        "pk": "id"
    },
    # add other tables here...
}

@pytest.fixture
def client():
    app.config['TESTING'] = True
    with app.test_client() as client:
        yield client

@pytest.mark.parametrize("table", TABLES.keys())
def test_get_all(client, table):
    """GET /<table> should return 200 and a JSON list."""
    resp = client.get(f"/{table}")
    assert resp.status_code == 200
    data = resp.get_json()
    assert isinstance(data, list)

@pytest.mark.parametrize("table,meta", TABLES.items())
def test_crud_lifecycle(client, table, meta):
    """Create → Read → Update → Delete cycle for each table."""
    example = meta["example"].copy()

    # 1) CREATE
    resp = client.post(f"/{table}", json=example)
    print("POST response:", resp.status_code, resp.data)  # Debug print
    assert resp.status_code in (200, 201)
    created = resp.get_json()
    pk = meta["pk"]
    assert pk in created
    record_id = created[pk]

    # 2) READ single
    resp = client.get(f"/{table}/{record_id}")
    assert resp.status_code == 200
    single = resp.get_json()
    for k, v in example.items():
        assert single[k] == v

    # 3) UPDATE
    updated = example.copy()
    # pick first field to modify
    first_field = list(example.keys())[0]
    # if numeric, add 1; if string, append "_u"
    if isinstance(updated[first_field], (int, float)):
        updated[first_field] += 1
    else:
        updated[first_field] = f"{updated[first_field]}_u"

    resp = client.put(f"/{table}/{record_id}", json=updated)
    assert resp.status_code == 200
    after_upd = resp.get_json()
    assert after_upd[first_field] == updated[first_field]

    # 4) DELETE
    resp = client.delete(f"/{table}/{record_id}")
    assert resp.status_code == 200

    # 5) VERIFY DELETE
    resp = client.get(f"/{table}/{record_id}")
    assert resp.status_code == 404  # or whatever your API returns for missing

