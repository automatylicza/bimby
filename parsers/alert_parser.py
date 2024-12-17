# parsers/alert_parser.py

from typing import List, Dict

class AlertParser:
    @staticmethod
    def parse(base_data: Dict, alert) -> List[Dict]:
        alert_data = []
        for period in alert.active_period:
            for informed_entity in alert.informed_entity:
                data = {
                    **base_data,
                    "alert_start": period.start,
                    "alert_end": period.end,
                    "cause": alert.cause,
                    "effect": alert.effect,
                    "url": AlertParser.get_translated_text(alert.url),
                    "header_text": AlertParser.get_translated_text(alert.header_text),
                    "description_text": AlertParser.get_translated_text(alert.description_text),
                    "agency_id": informed_entity.agency_id,
                    "route_id": informed_entity.route_id,
                    "stop_id": informed_entity.stop_id,
                    "trip_id": informed_entity.trip.trip_id if informed_entity.HasField("trip") else None
                }
                alert_data.append(data)
        return alert_data

    @staticmethod
    def get_translated_text(translated_string):
        if translated_string and translated_string.translation:
            return translated_string.translation[0].text
        return None
