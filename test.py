from openhexa.sdk import workspace
from pathlib import Path
from openhexa.toolbox.dhis2 import DHIS2

dhis2_connection = workspace.dhis2_connection("snis-sp-claire-api")
cache_dir = Path(workspace.files_path) / ".cache"
dhis2_client = DHIS2(connection=dhis2_connection, cache_dir=cache_dir)
url = "https://sp.snisrdc.com/api/29"
chunk = [
        {
            "dataElement": "pCbiHFTLwRX",
            "period": 202308,
            "orgUnit": "HdzPBv3jpme",
            "categoryOptionCombo": "HllvX50cXC0",
            "attributeOptionCombo": "HllvX50cXC0",
            "value": 1,
        },
        {
            "dataElement": "pCbiHFTLwRX",
            "period": 202308,
            "orgUnit": "cqgQ8nLcRoM",
            "categoryOptionCombo": "HllvX50cXC0",
            "attributeOptionCombo": "HllvX50cXC0",
            "value": 0,
        },
    ]
r = dhis2_client.api.session.post(
    f"{dhis2_client.api.url}/dataValueSets",
    json={"dataValues": chunk},
    params={
        "dryRun": False,
        "importStrategy": "CREATE_AND_UPDATE",
        "preheatCache": True,
        "skipAudit": True,
    },  # speed!
)

r.raise_for_status()
response = r.json()
print(response)