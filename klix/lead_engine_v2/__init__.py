"""
Lead Engine v2 package.

Responsible for:
- Ingesting leads from free / ToS-safe sources (OSM, open data, website crawler).
- Normalizing them into a common shape.
- Writing them into the `leads` table with dedupe keys and lineage.
"""
