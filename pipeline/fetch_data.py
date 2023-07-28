from io import BytesIO

import duckdb
import geopandas as gpd
import requests

PLACES_URI = "s3://overturemaps-us-west-2/release/2023-07-26-alpha.0/theme=places/type=*/*"
TOWNS_URL = "https://geoportal.dane.gov.co/descargas/mgn_2022/MGN2022_MPIO_POLITICO.zip"
HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.14; rv:66.0) Gecko/20100101 Firefox/66.0"}
TOWNS_COLUMNS = columns = {
    "MPIO_CDPMP": "town_id",
    "MPIO_CNMBR": "town_name",
    "DPTO_CNMBR": "state_name",
}


def fetch_places() -> gpd.GeoDataFrame:
    con = duckdb.connect(":memory:")
    con.execute("LOAD spatial;")
    con.execute("LOAD httpfs;")

    places = con.sql(
        f"""
        SELECT
            names['common'][1][1]['value'][1] AS name,
            categories.main AS category_main,
            categories.alternate AS category_alternate,
            ST_AsText(ST_GeomFromWKB(geometry)) as geometry
        FROM read_parquet('{PLACES_URI}', filename=true, hive_partitioning=1)
        WHERE
            bbox.minX > -81.822638
            AND bbox.maxX < -66.8523816
            AND bbox.minY > -4.2231578
            AND bbox.maxY < 13.4060535;
        """
    ).fetchdf()
    places = places.assign(geometry=gpd.GeoSeries.from_wkt(places.geometry))

    con.close()
    return gpd.GeoDataFrame(places, geometry="geometry", crs="EPSG:4326")


def fetch_towns() -> gpd.GeoDataFrame:
    res = requests.get(TOWNS_URL, headers=HEADERS)
    towns = gpd.read_file(BytesIO(res.content), include_fields=TOWNS_COLUMNS)
    towns = towns.rename(columns=TOWNS_COLUMNS)
    return towns.to_crs("EPSG:4326")


def main():
    places = fetch_places()
    towns = fetch_towns()
    
    tagged_places = places.sjoin(towns, how="inner")
    tagged_places.to_parquet("data/places.parquet", index=False)
    
if __name__ == "__main__":
    main()
    

