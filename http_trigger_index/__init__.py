import azure.functions as func
import logging
import pystac_client
import planetary_computer
import geopandas as gpd
import rioxarray
import sqlalchemy
from get_conn import get_connection_uri
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
import io
import os
import logging
import traceback

def load_band(item, band_name, match=None):
    band = rioxarray.open_rasterio(item.assets[band_name].href, overview_level=1).squeeze()
    band = band.astype("float32") / 10000.0
    # band = band.rio.clip(gdf.geometry.values, gdf.crs)
    if match is not None:
        band = band.rio.reproject_match(match)
    return band

def calc_index(index, selected_item):
    if index == "NDVI":
        red = load_band(selected_item, "B04")
        nir = load_band(selected_item, "B08")
        ndvi = (nir - red) / (nir + red)
        return ndvi
    elif index == "NDII":
        swir = load_band(selected_item, "B11")
        nir = load_band(selected_item, "B08", match=swir)
        ndii = (nir - swir) / (nir + swir)
        return ndii
    elif index == "NDBI":
        swir = load_band(selected_item, "B11")
        nir = load_band(selected_item, "B08", match=swir)
        ndbi = (swir - nir) / (swir + nir)
        return ndbi
    elif index == "NDWI":
        green = load_band(selected_item, "B03")
        nir = load_band(selected_item, "B08")
        ndwi = (green - nir) / (green + nir)
        return ndwi

def get_blob_service_client():
    account_name = os.getenv('AZURE_STORAGE_ACCOUNT')
    account_url = f"https://{account_name}.blob.core.windows.net"
    credential = DefaultAzureCredential()
    return BlobServiceClient(account_url, credential)

def blob_save(raster_data, blob_name, index, cmap):
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client("indeksy")

    if not container_client.exists():
        container_client.create_container()

    bytes_io = io.BytesIO()
    raster_data.rio.to_raster(bytes_io)
    bytes_io.seek(0)

    container_client.upload_blob(
        name=blob_name,
        data=bytes_io,
        overwrite=True,
        metadata={"index_type": index, "colormap": cmap}
    )


app = func.FunctionApp(http_auth_level=func.AuthLevel.ADMIN)

@app.route(route="http_trigger_index")
def http_trigger_index(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Wywołano funkcję http_trigger_index")

    try:
        catalog = pystac_client.Client.open(
        "https://planetarycomputer.microsoft.com/api/stac/v1",
        modifier=planetary_computer.sign_inplace,
        )
        time_range = "2024-04-01/2025-04-30"
        bbox = [16.8, 51.04, 17.17, 51.21]
        search = catalog.search(collections=["sentinel-2-l2a"], bbox=bbox, datetime=time_range)
        items = search.item_collection()

        selected_item = min(items, key=lambda item: item.properties["eo:cloud_cover"])

        index = req.params.get("index", "NDVI")
        cmap = req.params.get("cmap", "RdYlGn")

        index_data = calc_index(index, selected_item)
        blob_name = f"{index}_{cmap}.tif"
        blob_save(index_data, blob_name, index, cmap)

        return func.HttpResponse(blob_name, status_code=200)
    except Exception as e:
        logging.error("Wystąpił wyjątek: %s", str(e))
        logging.error(traceback.format_exc())
        return func.HttpResponse(f"Błąd: {str(e)}", status_code=500)
