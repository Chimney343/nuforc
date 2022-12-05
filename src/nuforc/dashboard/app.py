import geopandas as gpd
from greppo import app

events = gpd.read_file(r"nuforc_geojson.json")

app.base_layer(
    name="Open Street Map",
    visible=True,
    url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
    subdomains=None,
    attribution='(C) OpenStreetMap contributors',
)


app.vector_layer(
    data=events,
    name="UFO Events",
)


app.base_layer(provider="CartoDB Positron")
