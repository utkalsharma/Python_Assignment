import uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, AccessPolicy, ContainerSasPermissions, PublicAccess
import simplekml
import geojson
from shapely.geometry import shape
from decouple import config
from datetime import datetime, timedelta



def generate_and_upload_kml(body_data):

    # convert string into geojson and read coordinates
    # Create geometry from coordinates and check geom type
    get_geojson = geojson.loads(body_data)
    get_geometry = shape(get_geojson)
    geo_type = get_geometry.geom_type

    # check geom type and create KML file with styling
    kml = simplekml.Kml()

    if geo_type.upper() == ("MultiPolygon").upper():
        print("MultiPolygon not supported!")
        return {
            "message": "MultiPolygon not allowed!",
        }

    elif geo_type.upper() == ("Polygon").upper():
        attr_name = "A Polygon"
        coordinates = list(get_geometry.exterior.coords)
        ks = kml.newpolygon(name="A Polygon")
        ks.outerboundaryis = coordinates

    elif geo_type.upper() == ("Point").upper():
        attr_name = "A Point"
        coordinates = list(get_geometry.coords)
        ks = kml.newpoint(name=attr_name)
        ks.coords = coordinates

    elif geo_type.upper() == ("LineString").upper():
        attr_name = "A LineString"
        coordinates = list(get_geometry.coords)
        ks = kml.newlinestring(name=attr_name)
        ks.coords = coordinates

    else:
        return {
            "message": "Geojson type in not supported!",
        }

    # create kml styling
    ks.style.linestyle.color = simplekml.Color.green
    ks.style.linestyle.width = 5
    ks.style.polystyle.color = simplekml.Color.changealphaint(
        100, simplekml.Color.green
    )

    kml_data = kml.kml()
    get_blob_url = azure_kml_upload(kml_data)

    return {
        "kml_url": get_blob_url,
    }


def azure_kml_upload(kml_data: str) -> str:
    # Azure Operations
    try:
        # Create azure connection string
        connect_str = config('AZURE_STORAGE_CONNECTION_STRING')
    except Exception as ex:
        print('Exception:')
        print(ex)

    # Define Azure container name and kml file name
    container_name = "kml-storage"
    file_name = str(uuid.uuid4()) + ".kml"

    # Create the BlobServiceClient object which will be used to create a container client
    blob_service_client = BlobServiceClient.from_connection_string(connect_str)

    # Check if container already exists
    is_container_exists = exists(container_name)
    if is_container_exists == False:

        # Create the container
        container_client = blob_service_client.create_container(container_name)

        # Create access policy
        access_policy = AccessPolicy(permission=ContainerSasPermissions(read=True, write=True),
                                     expiry=datetime.utcnow() + timedelta(hours=1),
                                     start=datetime.utcnow() - timedelta(minutes=1))
        identifiers = {'read': access_policy}

        # Specifies full public read access for container and blob data.
        public_access = PublicAccess.Container

        # Set the access policy on the container
        container_client.set_container_access_policy(
            signed_identifiers=identifiers, public_access=public_access)

        blob = BlobClient.from_connection_string(
            conn_str=connect_str, container_name=container_name, blob_name=file_name)
    else:
        blob = BlobClient.from_connection_string(
            conn_str=connect_str, container_name=container_name, blob_name=file_name)

    blob.upload_blob(kml_data)
    blob_url = "https://geowgs84apis.blob.core.windows.net/kml-storage/" + file_name

    # Finally return newly created blob url
    return blob_url


def exists(container_name: str) -> bool:
    connect_str = config('AZURE_STORAGE_CONNECTION_STRING')
    container = ContainerClient.from_connection_string(
        connect_str, container_name)
    try:
        container_properties = container.get_container_properties()
    except Exception as e:
        return False
    return True
