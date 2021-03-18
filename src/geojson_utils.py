from typing import List, Dict, Type, Callable, Any, Union

from geojson import FeatureCollection, Feature, GeoJSON
from geojson.geometry import Geometry, LineString, Point
from zepben.evolve import PowerSystemResource, Location

__all__ = ["to_geojson_feature_collection", "to_geojson_feature", "to_geojson_geometry", "write_geojson_file"]


def write_geojson_file(filename: str, geojson: GeoJSON):
    f = open(filename, "w")
    f.write(str(geojson))
    f.close()


def to_geojson_feature_collection(
        psrs: List[PowerSystemResource],
        class_to_properties: Dict[Type, Dict[str, Callable[[Any], Any]]]
) -> FeatureCollection:
    features = []
    for psr in psrs:
        properties_map = class_to_properties.get(type(psr))

        if properties_map is not None:
            features.append(to_geojson_feature(psr, properties_map))

    return FeatureCollection(features)


def to_geojson_feature(
        psr: PowerSystemResource,
        property_map: Dict[str, Callable[[PowerSystemResource], Any]]
) -> Union[Feature, None]:
    geometry = to_geojson_geometry(psr.location)
    if geometry is None:
        return None

    properties = {k: f(psr) for (k, f) in property_map.items()}
    return Feature(psr.mrid, geometry, properties)


def to_geojson_geometry(location: Location) -> Union[Geometry, None]:
    points = list(location.points)
    if len(points) > 1:
        return LineString([(point.x_position, point.y_position) for point in points])
    elif len(points) == 1:
        return Point((points[0].x_position, points[0].y_position))
    else:
        return None
