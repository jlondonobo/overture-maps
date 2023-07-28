import geopandas as gpd

places = gpd.read_file('places_medellin.geojson')

places_points = places.assign(
    lat=places.geometry.y,
    lon=places.geometry.x,
    categories=lambda df: df['categories'].map({"bar": "Bar", "restaurant": "Restaurant", "beauty_salon": "Beauty Salon"})
)

places_points.to_file('places_medellin_points.geojson', driver='GeoJSON')