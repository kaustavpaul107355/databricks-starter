import { useEffect, useMemo, useState } from "react";
import { MapContainer, TileLayer, Marker, Popup } from "react-leaflet";
import L from "leaflet";
import "leaflet/dist/leaflet.css";
import markerIcon2x from "leaflet/dist/images/marker-icon-2x.png";
import markerIcon from "leaflet/dist/images/marker-icon.png";
import markerShadow from "leaflet/dist/images/marker-shadow.png";

L.Icon.Default.mergeOptions({
  iconRetinaUrl: markerIcon2x,
  iconUrl: markerIcon,
  shadowUrl: markerShadow,
});

type StoreLocation = {
  store_id: string;
  store_name: string;
  store_region: string;
  state: string;
  revenue: string;
  units: string;
  latitude: string;
  longitude: string;
};

const jitterForStore = (storeId: string) => {
  let hash = 0;
  for (let i = 0; i < storeId.length; i += 1) {
    hash = (hash << 5) - hash + storeId.charCodeAt(i);
    hash |= 0;
  }
  const normalized = Math.abs(hash % 1000) / 1000;
  const offset = (normalized - 0.5) * 0.5;
  return offset;
};

export function MapView() {
  const [locations, setLocations] = useState<StoreLocation[]>([]);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const [isLoading, setIsLoading] = useState(true);

  useEffect(() => {
    let mounted = true;
    setIsLoading(true);
    fetch("/api/dashboard/map")
      .then((response) => response.json())
      .then((payload) => {
        if (!mounted) {
          return;
        }
        const rows = (payload.locations || []) as StoreLocation[];
        setLocations(rows);
        setLastUpdated(new Date());
        setIsLoading(false);
      })
      .catch(() => {
        if (mounted) {
          setIsLoading(false);
        }
      });
    return () => {
      mounted = false;
    };
  }, []);

  const markers = useMemo(
    () =>
      locations
        .map((location) => ({
          ...location,
          lat: Number(location.latitude) + jitterForStore(`${location.store_id}-lat`),
          lng: Number(location.longitude) + jitterForStore(`${location.store_id}-lng`),
          revenueValue: Number(location.revenue || 0),
          unitsValue: Number(location.units || 0),
        }))
        .filter((location) => Number.isFinite(location.lat) && Number.isFinite(location.lng)),
    [locations]
  );

  const stats = useMemo(() => {
    const totalStores = markers.length;
    const totalRevenue = markers.reduce((sum, row) => sum + (row.revenueValue || 0), 0);
    const totalUnits = markers.reduce((sum, row) => sum + (row.unitsValue || 0), 0);
    const avgRevenue = totalStores ? totalRevenue / totalStores : 0;
    const avgUnits = totalStores ? totalUnits / totalStores : 0;
    const revenuePerUnit = totalUnits ? totalRevenue / totalUnits : 0;
    const topRevenueStore = [...markers].sort((a, b) => b.revenueValue - a.revenueValue)[0];
    const topUnitsStore = [...markers].sort((a, b) => b.unitsValue - a.unitsValue)[0];

    const revenueByState = markers.reduce<Record<string, number>>((acc, row) => {
      const key = row.state || "Unknown";
      acc[key] = (acc[key] || 0) + (row.revenueValue || 0);
      return acc;
    }, {});
    const revenueByRegion = markers.reduce<Record<string, number>>((acc, row) => {
      const key = row.store_region || "Unknown";
      acc[key] = (acc[key] || 0) + (row.revenueValue || 0);
      return acc;
    }, {});

    const topState = Object.entries(revenueByState).sort((a, b) => b[1] - a[1])[0];
    const topRegion = Object.entries(revenueByRegion).sort((a, b) => b[1] - a[1])[0];

    return [
      { label: "Total Stores", value: totalStores.toLocaleString() },
      { label: "Total Revenue", value: `$${totalRevenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}` },
      { label: "Avg Revenue / Store", value: `$${avgRevenue.toLocaleString(undefined, { maximumFractionDigits: 0 })}` },
      { label: "Total Units", value: totalUnits.toLocaleString() },
      { label: "Avg Units / Store", value: avgUnits.toLocaleString(undefined, { maximumFractionDigits: 0 }) },
      {
        label: "Top Store (Revenue)",
        value: topRevenueStore ? topRevenueStore.store_name : "—",
      },
      {
        label: "Top Store (Units)",
        value: topUnitsStore ? topUnitsStore.store_name : "—",
      },
      {
        label: "Top State (Revenue)",
        value: topState ? `${topState[0]} $${topState[1].toLocaleString(undefined, { maximumFractionDigits: 0 })}` : "—",
      },
      {
        label: "Top Region (Revenue)",
        value: topRegion ? `${topRegion[0]} $${topRegion[1].toLocaleString(undefined, { maximumFractionDigits: 0 })}` : "—",
      },
      {
        label: "Revenue / Unit",
        value: `$${revenuePerUnit.toLocaleString(undefined, { maximumFractionDigits: 2 })}`,
      },
    ];
  }, [markers]);

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-2xl font-semibold text-gray-900">Store Footprint Map</h2>
        <div className="text-xs text-gray-500">
          {isLoading ? "Loading live map..." : lastUpdated ? `Last updated ${lastUpdated.toLocaleTimeString()}` : ""}
        </div>
      </div>
      <div className="bg-white/80 backdrop-blur-sm rounded-xl shadow-sm border border-gray-200 p-4">
        <MapContainer center={[39.5, -98.35]} zoom={4} className="h-[520px] w-full rounded-lg">
          <TileLayer
            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {markers.map((location) => (
            <Marker
              key={`${location.store_id}-${location.state}`}
              position={[location.lat, location.lng]}
            >
              <Popup>
                <div className="space-y-1">
                  <div className="font-semibold">{location.store_name}</div>
                  <div className="text-sm text-gray-600">
                    {location.store_region} • {location.state}
                  </div>
                  <div className="text-sm text-gray-700">
                    Revenue: ${location.revenueValue.toLocaleString(undefined, { maximumFractionDigits: 0 })}
                  </div>
                  <div className="text-sm text-gray-700">Units: {location.unitsValue.toLocaleString()}</div>
                </div>
              </Popup>
            </Marker>
          ))}
        </MapContainer>
      </div>
      <div className="bg-white/80 backdrop-blur-sm rounded-xl shadow-sm border border-gray-200 p-6">
        <h3 className="text-lg font-semibold text-gray-900 mb-4">Store-Level Highlights</h3>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
          {stats.map((stat) => (
            <div key={stat.label} className="border border-gray-200 rounded-lg p-4 bg-white">
              <div className="text-xs text-gray-500">{stat.label}</div>
              <div className="text-lg font-semibold text-gray-900 mt-1">{stat.value}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
