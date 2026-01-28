import os

# Datasette base URL
DATASETTE_BASE_URL = os.getenv(
    "DATASETTE_BASE_URL", "https://datasette.planning.data.gov.uk/digital-land"
)

# Provision data source
PROVISION_CSV_URL = os.getenv(
    "PROVISION_CSV_URL",
    "https://raw.githubusercontent.com/digital-land/specification/refs/heads/main/specification/provision.csv",
)
