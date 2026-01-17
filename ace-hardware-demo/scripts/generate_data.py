import argparse
import csv
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import os
import random
import uuid


DEFAULT_BASE_TIME = "2026-01-14T00:00:00Z"
VENDOR_TYPES = ["ACE", "NON_ACE"]
ASN_STATUSES = ["CREATED", "SHIPPED", "IN_TRANSIT", "DELIVERED", "CANCELLED"]
CARRIERS = ["ACE_LOGISTICS", "FEDEX_FREIGHT", "XPO", "OLD_DOMINION", "ESTES"]

# Real US cities mapped to states, regions, and approximate GPS coordinates
CITIES_BY_REGION = {
    "MIDWEST": {
        "IL": [("Chicago", 41.8781, -87.6298), ("Aurora", 41.7606, -88.3201), ("Naperville", 41.7508, -88.1535), ("Joliet", 41.5250, -88.0817), ("Rockford", 42.2711, -89.0940), ("Springfield", 39.7817, -89.6501)],
        "IN": [("Indianapolis", 39.7684, -86.1581), ("Fort Wayne", 41.0793, -85.1394), ("Evansville", 37.9716, -87.5711), ("South Bend", 41.6764, -86.2520), ("Carmel", 39.9784, -86.1180)],
        "MI": [("Detroit", 42.3314, -83.0458), ("Grand Rapids", 42.9634, -85.6681), ("Warren", 42.5145, -83.0147), ("Sterling Heights", 42.5803, -83.0302), ("Ann Arbor", 42.2808, -83.7430), ("Lansing", 42.7325, -84.5555)],
        "OH": [("Columbus", 39.9612, -82.9988), ("Cleveland", 41.4993, -81.6944), ("Cincinnati", 39.1031, -84.5120), ("Toledo", 41.6528, -83.5379), ("Akron", 41.0814, -81.5190), ("Dayton", 39.7589, -84.1916)],
        "WI": [("Milwaukee", 43.0389, -87.9065), ("Madison", 43.0731, -89.4012), ("Green Bay", 44.5133, -88.0133), ("Kenosha", 42.5847, -87.8212), ("Racine", 42.7261, -87.7829)],
        "MN": [("Minneapolis", 44.9778, -93.2650), ("St. Paul", 44.9537, -93.0900), ("Rochester", 44.0121, -92.4802), ("Duluth", 46.7867, -92.1005), ("Bloomington", 44.8408, -93.2983)],
        "IA": [("Des Moines", 41.5868, -93.6250), ("Cedar Rapids", 41.9779, -91.6656), ("Davenport", 41.5236, -90.5776), ("Sioux City", 42.4959, -96.4003), ("Iowa City", 41.6611, -91.5302)],
        "MO": [("Kansas City", 39.0997, -94.5786), ("St. Louis", 38.6270, -90.1994), ("Springfield", 37.2090, -93.2923), ("Columbia", 38.9517, -92.3341), ("Independence", 39.0911, -94.4155)],
        "ND": [("Fargo", 46.8772, -96.7898), ("Bismarck", 46.8083, -100.7837), ("Grand Forks", 47.9253, -97.0329), ("Minot", 48.2330, -101.2963)],
        "SD": [("Sioux Falls", 43.5446, -96.7311), ("Rapid City", 44.0805, -103.2310), ("Aberdeen", 45.4647, -98.4865), ("Brookings", 44.3114, -96.7984)],
        "NE": [("Omaha", 41.2565, -95.9345), ("Lincoln", 40.8136, -96.7026), ("Bellevue", 41.1544, -95.8914), ("Grand Island", 40.9264, -98.3420)],
        "KS": [("Wichita", 37.6872, -97.3301), ("Overland Park", 38.9822, -94.6708), ("Kansas City", 39.1142, -94.6275), ("Topeka", 39.0473, -95.6752), ("Olathe", 38.8814, -94.8191)],
    },
    "SOUTH": {
        "TX": [("Houston", 29.7604, -95.3698), ("San Antonio", 29.4241, -98.4936), ("Dallas", 32.7767, -96.7970), ("Austin", 30.2672, -97.7431), ("Fort Worth", 32.7555, -97.3308), ("El Paso", 31.7619, -106.4850), ("Arlington", 32.7357, -97.1081), ("Corpus Christi", 27.8006, -97.3964)],
        "FL": [("Jacksonville", 30.3322, -81.6557), ("Miami", 25.7617, -80.1918), ("Tampa", 27.9506, -82.4572), ("Orlando", 28.5383, -81.3792), ("St. Petersburg", 27.7676, -82.6403), ("Hialeah", 25.8576, -80.2781), ("Tallahassee", 30.4383, -84.2807)],
        "GA": [("Atlanta", 33.7490, -84.3880), ("Augusta", 33.4735, -82.0105), ("Columbus", 32.4609, -84.9877), ("Macon", 32.8407, -83.6324), ("Savannah", 32.0809, -81.0912), ("Athens", 33.9519, -83.3576)],
        "NC": [("Charlotte", 35.2271, -80.8431), ("Raleigh", 35.7796, -78.6382), ("Greensboro", 36.0726, -79.7920), ("Durham", 35.9940, -78.8986), ("Winston-Salem", 36.0999, -80.2442), ("Fayetteville", 35.0527, -78.8784)],
        "VA": [("Virginia Beach", 36.8529, -75.9780), ("Norfolk", 36.8508, -76.2859), ("Chesapeake", 36.7682, -76.2875), ("Richmond", 37.5407, -77.4360), ("Newport News", 37.0871, -76.4730), ("Alexandria", 38.8048, -77.0469)],
        "TN": [("Nashville", 36.1627, -86.7816), ("Memphis", 35.1495, -90.0490), ("Knoxville", 35.9606, -83.9207), ("Chattanooga", 35.0456, -85.3097), ("Clarksville", 36.5298, -87.3595)],
        "LA": [("New Orleans", 29.9511, -90.0715), ("Baton Rouge", 30.4515, -91.1871), ("Shreveport", 32.5252, -93.7502), ("Lafayette", 30.2241, -92.0198), ("Lake Charles", 30.2266, -93.2174)],
        "KY": [("Louisville", 38.2527, -85.7585), ("Lexington", 38.0406, -84.5037), ("Bowling Green", 36.9685, -86.4808), ("Owensboro", 37.7742, -87.1117), ("Covington", 39.0837, -84.5086)],
        "SC": [("Charleston", 32.7765, -79.9311), ("Columbia", 34.0007, -81.0348), ("North Charleston", 32.8546, -79.9748), ("Mount Pleasant", 32.7932, -79.8624), ("Greenville", 34.8526, -82.3940)],
        "AL": [("Birmingham", 33.5186, -86.8104), ("Montgomery", 32.3668, -86.3000), ("Mobile", 30.6954, -88.0399), ("Huntsville", 34.7304, -86.5861), ("Tuscaloosa", 33.2098, -87.5692)],
        "MS": [("Jackson", 32.2988, -90.1848), ("Gulfport", 30.3674, -89.0928), ("Southaven", 34.9889, -90.0126), ("Hattiesburg", 31.3271, -89.2903), ("Biloxi", 30.3960, -88.8853)],
        "AR": [("Little Rock", 34.7465, -92.2896), ("Fort Smith", 35.3859, -94.3985), ("Fayetteville", 36.0626, -94.1574), ("Springdale", 36.1867, -94.1288), ("Jonesboro", 35.8423, -90.7043)],
        "OK": [("Oklahoma City", 35.4676, -97.5164), ("Tulsa", 36.1539, -95.9928), ("Norman", 35.2226, -97.4395), ("Broken Arrow", 36.0526, -95.7908), ("Edmond", 35.6528, -97.4781)],
    },
    "NORTHEAST": {
        "NY": [("New York City", 40.7128, -74.0060), ("Buffalo", 42.8864, -78.8784), ("Rochester", 43.1566, -77.6088), ("Yonkers", 40.9312, -73.8987), ("Syracuse", 43.0481, -76.1474), ("Albany", 42.6526, -73.7562)],
        "PA": [("Philadelphia", 39.9526, -75.1652), ("Pittsburgh", 40.4406, -79.9959), ("Allentown", 40.6084, -75.4902), ("Erie", 42.1292, -80.0851), ("Reading", 40.3356, -75.9269), ("Scranton", 41.4090, -75.6624)],
        "NJ": [("Newark", 40.7357, -74.1724), ("Jersey City", 40.7178, -74.0431), ("Paterson", 40.9168, -74.1718), ("Elizabeth", 40.6640, -74.2107), ("Edison", 40.5187, -74.4121), ("Trenton", 40.2171, -74.7429)],
        "MA": [("Boston", 42.3601, -71.0589), ("Worcester", 42.2626, -71.8023), ("Springfield", 42.1015, -72.5898), ("Cambridge", 42.3736, -71.1097), ("Lowell", 42.6334, -71.3162), ("Brockton", 42.0834, -71.0184)],
        "CT": [("Bridgeport", 41.1865, -73.1952), ("New Haven", 41.3083, -72.9279), ("Stamford", 41.0534, -73.5387), ("Hartford", 41.7658, -72.6734), ("Waterbury", 41.5582, -73.0515)],
        "RI": [("Providence", 41.8240, -71.4128), ("Warwick", 41.7001, -71.4162), ("Cranston", 41.7798, -71.4373), ("Pawtucket", 41.8787, -71.3828), ("East Providence", 41.8137, -71.3701)],
        "NH": [("Manchester", 42.9956, -71.4548), ("Nashua", 42.7654, -71.4676), ("Concord", 43.2081, -71.5376), ("Dover", 43.1979, -70.8737), ("Rochester", 43.3048, -70.9756)],
        "VT": [("Burlington", 44.4759, -73.2121), ("South Burlington", 44.4669, -73.1709), ("Rutland", 43.6106, -72.9726), ("Barre", 44.1970, -72.5020), ("Montpelier", 44.2601, -72.5754)],
        "ME": [("Portland", 43.6591, -70.2568), ("Lewiston", 44.1004, -70.2148), ("Bangor", 44.8016, -68.7712), ("South Portland", 43.6415, -70.2409), ("Auburn", 44.0979, -70.2311)],
    },
    "WEST": {
        "CA": [("Los Angeles", 34.0522, -118.2437), ("San Diego", 32.7157, -117.1611), ("San Jose", 37.3382, -121.8863), ("San Francisco", 37.7749, -122.4194), ("Fresno", 36.7378, -119.7871), ("Sacramento", 38.5816, -121.4944), ("Long Beach", 33.7701, -118.1937), ("Oakland", 37.8044, -122.2712)],
        "WA": [("Seattle", 47.6062, -122.3321), ("Spokane", 47.6588, -117.4260), ("Tacoma", 47.2529, -122.4443), ("Vancouver", 45.6387, -122.6615), ("Bellevue", 47.6101, -122.2015), ("Everett", 47.9790, -122.2021)],
        "OR": [("Portland", 45.5152, -122.6784), ("Salem", 44.9429, -123.0351), ("Eugene", 44.0521, -123.0868), ("Gresham", 45.4981, -122.4302), ("Hillsboro", 45.5229, -122.9898), ("Beaverton", 45.4871, -122.8037)],
        "AZ": [("Phoenix", 33.4484, -112.0740), ("Tucson", 32.2226, -110.9747), ("Mesa", 33.4152, -111.8315), ("Chandler", 33.3062, -111.8413), ("Scottsdale", 33.4942, -111.9261), ("Glendale", 33.5387, -112.1860)],
        "CO": [("Denver", 39.7392, -104.9903), ("Colorado Springs", 38.8339, -104.8214), ("Aurora", 39.7294, -104.8319), ("Fort Collins", 40.5853, -105.0844), ("Lakewood", 39.7047, -105.0814)],
        "NV": [("Las Vegas", 36.1699, -115.1398), ("Henderson", 36.0395, -114.9817), ("Reno", 39.5296, -119.8138), ("North Las Vegas", 36.1989, -115.1175), ("Sparks", 39.5349, -119.7527)],
        "UT": [("Salt Lake City", 40.7608, -111.8910), ("West Valley City", 40.6916, -112.0011), ("Provo", 40.2338, -111.6585), ("West Jordan", 40.6097, -111.9391), ("Orem", 40.2969, -111.6946)],
        "ID": [("Boise", 43.6150, -116.2023), ("Meridian", 43.6121, -116.3915), ("Nampa", 43.5407, -116.5635), ("Idaho Falls", 43.4666, -112.0341), ("Pocatello", 42.8713, -112.4455)],
        "MT": [("Billings", 45.7833, -108.5007), ("Missoula", 46.8721, -113.9940), ("Great Falls", 47.5053, -111.3008), ("Bozeman", 45.6770, -111.0429), ("Butte", 46.0038, -112.5348)],
        "WY": [("Cheyenne", 41.1400, -104.8202), ("Casper", 42.8666, -106.3131), ("Laramie", 41.3114, -105.5911), ("Gillette", 44.2911, -105.5022), ("Rock Springs", 41.5875, -109.2029)],
        "NM": [("Albuquerque", 35.0844, -106.6504), ("Las Cruces", 32.3199, -106.7637), ("Rio Rancho", 35.2328, -106.6989), ("Santa Fe", 35.6870, -105.9378), ("Roswell", 33.3943, -104.5230)],
        "HI": [("Honolulu", 21.3099, -157.8581), ("Pearl City", 21.3972, -157.9753), ("Hilo", 19.7071, -155.0857), ("Kailua", 21.4022, -157.7394), ("Waipahu", 21.3861, -158.0092)],
        "AK": [("Anchorage", 61.2181, -149.9003), ("Fairbanks", 64.8378, -147.7164), ("Juneau", 58.3019, -134.4197), ("Sitka", 57.0531, -135.3300), ("Ketchikan", 55.3422, -131.6461)],
    },
}

PRODUCT_CATEGORIES = [
    "POWER_TOOLS", "HAND_TOOLS", "LAWN_GARDEN", "PAINT", "PLUMBING",
    "ELECTRICAL", "HARDWARE", "BUILDING_MATERIALS", "SEASONAL"
]

EVENT_TYPES = [
    "SHIPMENT_CREATED", "DEPARTED_WAREHOUSE", "IN_TRANSIT", 
    "ARRIVED_DC", "OUT_FOR_DELIVERY", "DELIVERED", "EXCEPTION"
]

DELAY_REASONS = [
    "WEATHER", "TRAFFIC", "MECHANICAL_FAILURE", "DRIVER_SHORTAGE",
    "CUSTOMS_DELAY", "ROUTE_OPTIMIZATION", "LOADING_DELAY", "NONE"
]


@dataclass
class Vendor:
    vendor_id: str
    vendor_name: str
    vendor_type: str
    risk_tier: str
    on_time_pct: float


@dataclass
class Store:
    store_id: int
    store_name: str
    region_id: str
    city: str
    state: str
    latitude: float
    longitude: float
    open_date: str
    is_active: bool
    weekly_revenue: float


@dataclass
class Shipment:
    shipment_id: str
    vendor_id: str
    store_id: int
    origin_city: str
    origin_state: str
    origin_latitude: float
    origin_longitude: float
    planned_departure_ts: str
    planned_arrival_ts: str
    asn_status: str
    carrier: str
    total_value: float


@dataclass
class Product:
    sku: str
    product_name: str
    category: str
    unit_price: float
    requires_temp_control: bool


def parse_iso_datetime(value: str) -> datetime:
    if value.endswith("Z"):
        value = f"{value[:-1]}+00:00"
    return datetime.fromisoformat(value)


def to_csv_value(value):
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    return value


def write_csv(path: str, header, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as file_handle:
        writer = csv.writer(file_handle)
        writer.writerow(header)
        for row in rows:
            writer.writerow([to_csv_value(value) for value in row])


def generate_products(rng: random.Random, num_products: int = 500):
    products = []
    for i in range(num_products):
        sku = f"SKU-{i + 1:05d}"
        category = rng.choice(PRODUCT_CATEGORIES)
        unit_price = round(rng.uniform(5.99, 299.99), 2)
        requires_temp = category in ["PAINT", "SEASONAL"] and rng.random() < 0.2
        product = Product(
            sku=sku,
            product_name=f"{category.replace('_', ' ').title()}-{i + 1:03d}",
            category=category,
            unit_price=unit_price,
            requires_temp_control=requires_temp,
        )
        products.append(product)
    return products


def generate_vendors(rng: random.Random, num_vendors: int):
    vendors = []
    risk_tiers = ["LOW", "MEDIUM", "HIGH"]
    for i in range(num_vendors):
        vendor_id = f"VEND-{i + 1:04d}"
        vendor_type = rng.choices(VENDOR_TYPES, weights=[0.6, 0.4])[0]
        risk_tier = rng.choice(risk_tiers)
        # ACE vendors have better on-time performance
        base_on_time = 0.92 if vendor_type == "ACE" else 0.78
        on_time_pct = round(base_on_time + rng.uniform(-0.1, 0.08), 3)
        vendor = Vendor(
            vendor_id=vendor_id,
            vendor_name=f"{vendor_type}-VENDOR-{i + 1:03d}",
            vendor_type=vendor_type,
            risk_tier=risk_tier,
            on_time_pct=on_time_pct,
        )
        vendors.append(vendor)
    return vendors


def generate_stores(rng: random.Random, num_stores: int, base_time: datetime):
    stores = []
    regions = list(CITIES_BY_REGION.keys())
    
    for i in range(num_stores):
        store_id = 1000 + i
        region_id = rng.choice(regions)
        
        # Pick a random state within the region
        state = rng.choice(list(CITIES_BY_REGION[region_id].keys()))
        
        # Pick a random city within that state (with GPS coordinates)
        city_data = rng.choice(CITIES_BY_REGION[region_id][state])
        city, latitude, longitude = city_data
        
        open_date = (base_time - timedelta(days=rng.randint(200, 4000))).date().isoformat()
        weekly_revenue = round(rng.uniform(50000, 500000), 2)
        store = Store(
            store_id=store_id,
            store_name=f"ACE-{city.upper().replace(' ', '-')}-{store_id}",
            region_id=region_id,
            city=city,
            state=state,
            latitude=latitude,
            longitude=longitude,
            open_date=open_date,
            is_active=rng.random() > 0.02,
            weekly_revenue=weekly_revenue,
        )
        stores.append(store)
    return stores


def generate_shipments(
    rng: random.Random,
    num_shipments: int,
    vendors,
    stores,
    products,
    base_time: datetime,
):
    shipments = []
    all_regions = list(CITIES_BY_REGION.keys())
    
    for i in range(num_shipments):
        shipment_id = f"SHIP-{10000 + i}"
        vendor = rng.choice(vendors)
        store = rng.choice(stores)
        carrier = rng.choice(CARRIERS)
        planned_departure = base_time + timedelta(hours=rng.randint(-24, 72))
        transit_hours = rng.randint(12, 72)
        planned_arrival = planned_departure + timedelta(hours=transit_hours)
        
        # Pick origin from a random region/state with GPS
        origin_region = rng.choice(all_regions)
        origin_state = rng.choice(list(CITIES_BY_REGION[origin_region].keys()))
        origin_city_data = rng.choice(CITIES_BY_REGION[origin_region][origin_state])
        origin_city, origin_lat, origin_lon = origin_city_data
        
        # Calculate shipment value from random products
        num_line_items = rng.randint(3, 15)
        total_value = 0
        for _ in range(num_line_items):
            product = rng.choice(products)
            qty = rng.randint(10, 500)
            total_value += product.unit_price * qty
        
        shipment = Shipment(
            shipment_id=shipment_id,
            vendor_id=vendor.vendor_id,
            store_id=store.store_id,
            origin_city=origin_city,
            origin_state=origin_state,
            origin_latitude=origin_lat,
            origin_longitude=origin_lon,
            planned_departure_ts=planned_departure.isoformat(),
            planned_arrival_ts=planned_arrival.isoformat(),
            asn_status=rng.choice(ASN_STATUSES),
            carrier=carrier,
            total_value=round(total_value, 2),
        )
        shipments.append(shipment)
    return shipments


def generate_shipment_line_items(
    rng: random.Random,
    shipments,
    products,
):
    line_items = []
    for shipment in shipments:
        num_lines = rng.randint(3, 15)
        selected_products = rng.sample(products, min(num_lines, len(products)))
        for idx, product in enumerate(selected_products):
            qty = rng.randint(10, 500)
            line_total = round(product.unit_price * qty, 2)
            line_items.append([
                shipment.shipment_id,
                idx + 1,
                product.sku,
                qty,
                product.unit_price,
                line_total,
            ])
    return line_items


def generate_logistics_events(
    rng: random.Random,
    num_shipments_to_track: int,
    shipments,
    vendors_by_id,
    stores_by_id,
    base_time: datetime,
):
    """Generate multiple tracking events per shipment with realistic GPS coordinates along route"""
    events = []
    
    # Select subset of shipments to track
    tracked_shipments = rng.sample(shipments, min(num_shipments_to_track, len(shipments)))
    
    def interpolate_gps(origin_lat, origin_lon, dest_lat, dest_lon, progress):
        """Linear interpolation between origin and destination"""
        lat = origin_lat + (dest_lat - origin_lat) * progress
        lon = origin_lon + (dest_lon - origin_lon) * progress
        # Add small random jitter for realistic GPS noise
        lat += rng.uniform(-0.05, 0.05)
        lon += rng.uniform(-0.05, 0.05)
        return round(lat, 6), round(lon, 6)
    
    for shipment in tracked_shipments:
        vendor = vendors_by_id[shipment.vendor_id]
        store = stores_by_id[shipment.store_id]
        truck_id = f"TRUCK-{rng.randint(100, 999)}"
        
        planned_departure = parse_iso_datetime(shipment.planned_departure_ts)
        planned_arrival = parse_iso_datetime(shipment.planned_arrival_ts)
        
        # Determine if shipment will be delayed based on vendor performance
        is_delayed = rng.random() > vendor.on_time_pct
        delay_minutes = rng.randint(30, 480) if is_delayed else 0
        delay_reason = rng.choice([r for r in DELAY_REASONS if r != "NONE"]) if is_delayed else "NONE"
        
        # Generate event sequence with GPS interpolation
        event_sequence = [
            ("SHIPMENT_CREATED", planned_departure - timedelta(hours=24), 0.0),
            ("DEPARTED_WAREHOUSE", planned_departure, 0.0),
            ("IN_TRANSIT", planned_departure + timedelta(hours=rng.randint(4, 12)), 0.25),
            ("ARRIVED_DC", planned_arrival - timedelta(hours=8), 0.75),
            ("OUT_FOR_DELIVERY", planned_arrival - timedelta(hours=2), 0.90),
            ("DELIVERED", planned_arrival + timedelta(minutes=delay_minutes), 1.0),
        ]
        
        for event_type, event_ts, route_progress in event_sequence:
            # Skip some events randomly to simulate incomplete tracking
            if event_type not in ["SHIPMENT_CREATED", "DELIVERED"] and rng.random() < 0.15:
                continue
                
            event_id = str(uuid.uuid4())
            
            # Determine status
            if event_type == "DELIVERED":
                status = "DELAYED" if is_delayed else "ON_TIME"
            elif event_type in ["DEPARTED_WAREHOUSE", "IN_TRANSIT", "OUT_FOR_DELIVERY"]:
                status = "IN_TRANSIT"
            else:
                status = "PENDING"
            
            # GPS coordinates - interpolate along route from origin to destination
            lat, lon = interpolate_gps(
                shipment.origin_latitude,
                shipment.origin_longitude,
                store.latitude,
                store.longitude,
                route_progress
            )
            
            # Temperature for sensitive goods
            temp_celsius = None
            if rng.random() < 0.3:  # 30% of shipments have temp monitoring
                temp_celsius = round(rng.uniform(18.0, 25.0), 1)
                if is_delayed and event_type == "IN_TRANSIT":
                    temp_celsius += rng.uniform(2.0, 8.0)  # Temp spike during delay
                temp_celsius = round(temp_celsius, 1)
            
            # Inject nulls for bad data quality
            store_id_value = None if rng.random() < 0.03 else store.store_id
            vendor_id_value = None if rng.random() < 0.02 else vendor.vendor_id
            
            events.append([
                event_id,
                truck_id,
                shipment.shipment_id,
                store_id_value,
                store.region_id,
                vendor.vendor_type,
                vendor_id_value,
                event_ts.isoformat(),
                lat,
                lon,
                planned_arrival.isoformat(),
                event_ts.isoformat() if event_type == "DELIVERED" else None,
                status,
                delay_minutes if event_type == "DELIVERED" and is_delayed else None,
                event_ts.date().isoformat(),
                event_type,
                delay_reason,
                shipment.carrier,
                temp_celsius,
                shipment.total_value,
            ])
    
    return events


def main():
    parser = argparse.ArgumentParser(description="Generate enriched Ace demo datasets.")
    parser.add_argument("--num-shipments", type=int, default=1200)
    parser.add_argument("--num-events", type=int, default=1000, help="Number of shipments to generate tracking events for")
    parser.add_argument("--num-stores", type=int, default=250)
    parser.add_argument("--num-vendors", type=int, default=40)
    parser.add_argument("--num-products", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--base-time", type=str, default=DEFAULT_BASE_TIME)
    parser.add_argument("--output-dir", type=str, default="data")
    args = parser.parse_args()

    base_time = parse_iso_datetime(args.base_time)
    if base_time.tzinfo is None:
        base_time = base_time.replace(tzinfo=timezone.utc)

    rng = random.Random(args.seed)

    print("Generating products...")
    products = generate_products(rng, args.num_products)
    
    print("Generating vendors...")
    vendors = generate_vendors(rng, args.num_vendors)
    vendors_by_id = {v.vendor_id: v for v in vendors}

    print("Generating stores...")
    stores = generate_stores(rng, args.num_stores, base_time)
    stores_by_id = {s.store_id: s for s in stores}

    print("Generating shipments...")
    shipments = generate_shipments(
        rng, args.num_shipments, vendors, stores, products, base_time
    )

    print("Generating shipment line items...")
    line_items = generate_shipment_line_items(rng, shipments, products)

    print("Generating logistics tracking events...")
    telemetry = generate_logistics_events(
        rng, args.num_events, shipments, vendors_by_id, stores_by_id, base_time
    )

    # Write products
    write_csv(
        os.path.join(args.output_dir, "dimensions", "products.csv"),
        ["sku", "product_name", "category", "unit_price", "requires_temp_control"],
        [[p.sku, p.product_name, p.category, p.unit_price, p.requires_temp_control] for p in products],
    )

    # Write vendors
    write_csv(
        os.path.join(args.output_dir, "dimensions", "vendors.csv"),
        ["vendor_id", "vendor_name", "vendor_type", "risk_tier", "on_time_pct"],
        [[v.vendor_id, v.vendor_name, v.vendor_type, v.risk_tier, v.on_time_pct] for v in vendors],
    )

    # Write stores
    write_csv(
        os.path.join(args.output_dir, "dimensions", "stores.csv"),
        ["store_id", "store_name", "region_id", "city", "state", "latitude", "longitude", "open_date", "is_active", "weekly_revenue"],
        [[s.store_id, s.store_name, s.region_id, s.city, s.state, s.latitude, s.longitude, s.open_date, s.is_active, s.weekly_revenue] for s in stores],
    )

    # Write shipments
    write_csv(
        os.path.join(args.output_dir, "dimensions", "shipments.csv"),
        ["shipment_id", "vendor_id", "store_id", "origin_city", "origin_state", "origin_latitude", "origin_longitude",
         "planned_departure_ts", "planned_arrival_ts", "asn_status", "carrier", "total_value"],
        [[sh.shipment_id, sh.vendor_id, sh.store_id, sh.origin_city, sh.origin_state, sh.origin_latitude, sh.origin_longitude,
          sh.planned_departure_ts, sh.planned_arrival_ts, sh.asn_status, sh.carrier, sh.total_value] for sh in shipments],
    )

    # Write shipment line items
    write_csv(
        os.path.join(args.output_dir, "dimensions", "shipment_line_items.csv"),
        ["shipment_id", "line_number", "sku", "quantity", "unit_price", "line_total"],
        line_items,
    )

    # Write telemetry
    write_csv(
        os.path.join(args.output_dir, "telemetry", "logistics_telemetry.csv"),
        [
            "event_id", "truck_id", "shipment_id", "store_id", "region_id",
            "vendor_type", "vendor_id", "event_ts", "latitude", "longitude",
            "estimated_arrival_ts", "actual_arrival_ts", "shipment_status",
            "delay_minutes", "ingest_date", "event_type", "delay_reason",
            "carrier", "temperature_celsius", "shipment_value"
        ],
        telemetry,
    )

    print(f"\n✅ Generated {len(products)} products")
    print(f"✅ Generated {len(vendors)} vendors")
    print(f"✅ Generated {len(stores)} stores")
    print(f"✅ Generated {len(shipments)} shipments")
    print(f"✅ Generated {len(line_items)} shipment line items")
    print(f"✅ Generated {len(telemetry)} tracking events")
    print(f"\nFiles written to: {args.output_dir}/")


if __name__ == "__main__":
    main()
