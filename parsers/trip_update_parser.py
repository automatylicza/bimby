# parsers/trip_update_parser.py

from typing import List, Dict

class TripUpdateParser:
    @staticmethod
    def parse(base_data: Dict, trip_update) -> List[Dict]:
        trip_data = []
        trip = trip_update.trip
        for stop_time in trip_update.stop_time_update:
            trip_data.append({
                **base_data,
                "trip_id": trip.trip_id,
                "route_id": trip.route_id,
                "start_time": trip.start_time,
                "start_date": trip.start_date,
                "stop_sequence": stop_time.stop_sequence,
                "stop_id": stop_time.stop_id,
                "arrival_delay": getattr(stop_time.arrival, "delay", None),
                "departure_delay": getattr(stop_time.departure, "delay", None),
                "schedule_relationship": stop_time.schedule_relationship,
                "timestamp": trip_update.timestamp,
                "delay": trip_update.delay
            })
        return trip_data
