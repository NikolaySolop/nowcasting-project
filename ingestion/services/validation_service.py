from typing import TypeVar

from ingestion.schemas.observations import ObservationIn, ParsedObservation


ObservationT = TypeVar("ObservationT", ParsedObservation, ObservationIn)


class ValidationService:
    def deduplicate_batch(self, observations: list[ObservationT]) -> tuple[list[ObservationT], int]:
        seen: set[tuple[object, ...]] = set()
        unique: list[ObservationT] = []
        duplicate_count = 0

        for observation in observations:
            key = observation.duplicate_key
            if key in seen:
                duplicate_count += 1
                continue
            seen.add(key)
            unique.append(observation)

        return unique, duplicate_count
