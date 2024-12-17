# parsers/vehicle_position_parser.py

from typing import Dict

class VehiclePositionParser:
    @staticmethod
    def parse(base_data: Dict, vehicle) -> Dict:
        position = vehicle.position
        return {
            **base_data,
            "trip_id": vehicle.trip.trip_id if vehicle.trip.HasField('trip_id') else None,
            "latitude": position.latitude if position.HasField('latitude') else None,
            "longitude": position.longitude if position.HasField('longitude') else None,
            "speed": position.speed if position.HasField('speed') else None,
            "bearing": position.bearing if position.HasField('bearing') else None,
            "occupancy_status": vehicle.occupancy_status,
            "timestamp": vehicle.timestamp
        }