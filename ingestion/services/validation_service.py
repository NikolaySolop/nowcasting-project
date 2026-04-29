from ingestion.schemas.observations import RawObservationIn


class ValidationService:
    def deduplicate_batch(self, observations: list[RawObservationIn]) -> tuple[list[RawObservationIn], int]:
        seen: set[tuple[object, ...]] = set()
        unique: list[RawObservationIn] = []
        duplicate_count = 0

        for observation in observations:
            key = observation.duplicate_key
            if key in seen:
                duplicate_count += 1
                continue
            seen.add(key)
            unique.append(observation)

        return unique, duplicate_count
