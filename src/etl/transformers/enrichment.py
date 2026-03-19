"""Data enrichment transformer for trade/customs data.

Provides two built-in enrichment strategies:

1. **HS code lookup** -- maps raw HS codes to chapter descriptions using
   the standard Harmonized System classification hierarchy.
2. **Country normalisation** -- maps variant country names and ISO-2/ISO-3
   codes to a canonical (ISO-3166 alpha-3, full name) representation.

Both are optional and controlled via constructor flags.
"""

from __future__ import annotations

from typing import Any

import pandas as pd
import structlog

from etl.transformers.base import BaseTransformer

logger = structlog.get_logger(__name__)

# HS code chapter descriptions (chapters 01-99).
# In production this would be loaded from a reference database; here we
# include the most common chapters for customs data processing.
_HS_CHAPTER_DESCRIPTIONS: dict[str, str] = {
    "01": "Live animals",
    "02": "Meat and edible meat offal",
    "03": "Fish and crustaceans",
    "04": "Dairy produce; eggs; honey",
    "05": "Products of animal origin",
    "06": "Live trees and other plants",
    "07": "Edible vegetables",
    "08": "Edible fruit and nuts",
    "09": "Coffee, tea, mate and spices",
    "10": "Cereals",
    "11": "Milling industry products",
    "12": "Oil seeds and oleaginous fruits",
    "15": "Animal or vegetable fats and oils",
    "16": "Preparations of meat or fish",
    "17": "Sugars and sugar confectionery",
    "18": "Cocoa and cocoa preparations",
    "19": "Preparations of cereals",
    "20": "Preparations of vegetables or fruit",
    "21": "Miscellaneous edible preparations",
    "22": "Beverages, spirits and vinegar",
    "23": "Residues from food industries",
    "24": "Tobacco and manufactured substitutes",
    "25": "Salt; sulphur; earths and stone",
    "26": "Ores, slag and ash",
    "27": "Mineral fuels and oils",
    "28": "Inorganic chemicals",
    "29": "Organic chemicals",
    "30": "Pharmaceutical products",
    "31": "Fertilisers",
    "32": "Tanning or dyeing extracts",
    "33": "Essential oils and cosmetics",
    "34": "Soap and washing preparations",
    "35": "Albuminoidal substances; glues",
    "37": "Photographic goods",
    "38": "Miscellaneous chemical products",
    "39": "Plastics and articles thereof",
    "40": "Rubber and articles thereof",
    "41": "Raw hides and skins",
    "42": "Articles of leather",
    "44": "Wood and articles of wood",
    "47": "Pulp of wood",
    "48": "Paper and paperboard",
    "49": "Printed books and newspapers",
    "50": "Silk",
    "51": "Wool and fine animal hair",
    "52": "Cotton",
    "54": "Man-made filaments",
    "55": "Man-made staple fibres",
    "61": "Articles of apparel (knitted)",
    "62": "Articles of apparel (not knitted)",
    "63": "Other made-up textile articles",
    "64": "Footwear",
    "68": "Articles of stone, plaster, cement",
    "69": "Ceramic products",
    "70": "Glass and glassware",
    "71": "Precious metals and stones",
    "72": "Iron and steel",
    "73": "Articles of iron or steel",
    "74": "Copper and articles thereof",
    "75": "Nickel and articles thereof",
    "76": "Aluminium and articles thereof",
    "82": "Tools of base metal",
    "83": "Miscellaneous articles of base metal",
    "84": "Machinery and mechanical appliances",
    "85": "Electrical machinery and equipment",
    "86": "Railway locomotives",
    "87": "Vehicles (not railway)",
    "88": "Aircraft and spacecraft",
    "89": "Ships, boats and floating structures",
    "90": "Optical and precision instruments",
    "91": "Clocks and watches",
    "94": "Furniture; bedding; lamps",
    "95": "Toys, games and sports equipment",
    "96": "Miscellaneous manufactured articles",
    "97": "Works of art and antiques",
    "99": "Special classification provisions",
}

# Country name normalisation map -- maps common variants and ISO codes
# to canonical (alpha-3, full name) pairs.
_COUNTRY_ALIASES: dict[str, tuple[str, str]] = {
    # ISO alpha-2
    "US": ("USA", "United States"),
    "GB": ("GBR", "United Kingdom"),
    "DE": ("DEU", "Germany"),
    "FR": ("FRA", "France"),
    "CN": ("CHN", "China"),
    "JP": ("JPN", "Japan"),
    "KR": ("KOR", "South Korea"),
    "IN": ("IND", "India"),
    "BR": ("BRA", "Brazil"),
    "TR": ("TUR", "Turkey"),
    "AZ": ("AZE", "Azerbaijan"),
    "RU": ("RUS", "Russia"),
    "GE": ("GEO", "Georgia"),
    "IT": ("ITA", "Italy"),
    "ES": ("ESP", "Spain"),
    "NL": ("NLD", "Netherlands"),
    "BE": ("BEL", "Belgium"),
    "PL": ("POL", "Poland"),
    "SE": ("SWE", "Sweden"),
    "AE": ("ARE", "United Arab Emirates"),
    "SA": ("SAU", "Saudi Arabia"),
    "SG": ("SGP", "Singapore"),
    # ISO alpha-3 passthrough
    "USA": ("USA", "United States"),
    "GBR": ("GBR", "United Kingdom"),
    "DEU": ("DEU", "Germany"),
    "FRA": ("FRA", "France"),
    "CHN": ("CHN", "China"),
    "JPN": ("JPN", "Japan"),
    "KOR": ("KOR", "South Korea"),
    "IND": ("IND", "India"),
    "BRA": ("BRA", "Brazil"),
    "TUR": ("TUR", "Turkey"),
    "AZE": ("AZE", "Azerbaijan"),
    "RUS": ("RUS", "Russia"),
    "GEO": ("GEO", "Georgia"),
    # Common name variants
    "UNITED STATES": ("USA", "United States"),
    "UNITED STATES OF AMERICA": ("USA", "United States"),
    "UNITED KINGDOM": ("GBR", "United Kingdom"),
    "GREAT BRITAIN": ("GBR", "United Kingdom"),
    "PEOPLES REPUBLIC OF CHINA": ("CHN", "China"),
    "CHINA": ("CHN", "China"),
    "GERMANY": ("DEU", "Germany"),
    "FRANCE": ("FRA", "France"),
    "JAPAN": ("JPN", "Japan"),
    "SOUTH KOREA": ("KOR", "South Korea"),
    "REPUBLIC OF KOREA": ("KOR", "South Korea"),
    "INDIA": ("IND", "India"),
    "BRAZIL": ("BRA", "Brazil"),
    "TURKEY": ("TUR", "Turkey"),
    "TURKIYE": ("TUR", "Turkey"),
    "AZERBAIJAN": ("AZE", "Azerbaijan"),
    "RUSSIA": ("RUS", "Russia"),
    "RUSSIAN FEDERATION": ("RUS", "Russia"),
    "GEORGIA": ("GEO", "Georgia"),
    "UAE": ("ARE", "United Arab Emirates"),
    "UNITED ARAB EMIRATES": ("ARE", "United Arab Emirates"),
}


class EnrichmentTransformer(BaseTransformer):
    """Enrich trade data with HS code descriptions and normalised country names.

    Args:
        hs_code_column: Name of the column containing HS codes.
        country_columns: Columns containing country identifiers to normalise.
        enrich_hs_codes: Whether to add HS chapter description columns.
        normalize_countries: Whether to normalise country columns.
        hs_lookup: Optional custom HS code lookup dict. If ``None``,
            uses the built-in chapter-level descriptions.
        country_lookup: Optional custom country alias dict.
    """

    def __init__(
        self,
        hs_code_column: str = "hs_code",
        country_columns: list[str] | None = None,
        enrich_hs_codes: bool = True,
        normalize_countries: bool = True,
        hs_lookup: dict[str, str] | None = None,
        country_lookup: dict[str, tuple[str, str]] | None = None,
    ) -> None:
        self.hs_code_column = hs_code_column
        self.country_columns = country_columns or ["country_origin", "country_destination"]
        self.enrich_hs_codes = enrich_hs_codes
        self.normalize_countries = normalize_countries
        self._hs_lookup = hs_lookup or _HS_CHAPTER_DESCRIPTIONS
        self._country_lookup = country_lookup or _COUNTRY_ALIASES

    def transform(self, df: pd.DataFrame) -> tuple[pd.DataFrame, int]:
        """Apply enrichment to the DataFrame.

        Enrichment never rejects records -- it only adds or modifies
        columns. The rejected count is always 0.

        Returns:
            Tuple of (enriched_df, 0).
        """
        if df.empty:
            return df, 0

        result = df.copy()
        enriched_count = 0

        if self.enrich_hs_codes and self.hs_code_column in result.columns:
            result = self._enrich_hs_codes(result)
            enriched_count += 1

        if self.normalize_countries:
            for col in self.country_columns:
                if col in result.columns:
                    result = self._normalize_country_column(result, col)
                    enriched_count += 1

        logger.info(
            "enrichment_completed",
            enrichments_applied=enriched_count,
            rows=len(result),
        )
        return result, 0

    def _enrich_hs_codes(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add HS chapter code and description columns."""
        hs_series = df[self.hs_code_column].astype(str).str.strip()

        # Extract 2-digit chapter from the HS code
        chapters = hs_series.str[:2].str.zfill(2)

        df["hs_chapter"] = chapters
        df["hs_chapter_description"] = chapters.map(self._hs_lookup).fillna("Unknown")

        mapped_count = (df["hs_chapter_description"] != "Unknown").sum()
        logger.debug(
            "hs_code_enrichment",
            total=len(df),
            mapped=int(mapped_count),
            unmapped=int(len(df) - mapped_count),
        )
        return df

    def _normalize_country_column(
        self,
        df: pd.DataFrame,
        column: str,
    ) -> pd.DataFrame:
        """Normalise a country column to ISO-3 code and full name."""
        raw = df[column].astype(str).str.strip().str.upper()

        iso3_col = f"{column}_iso3"
        name_col = f"{column}_name"

        def _lookup(value: str) -> tuple[str, str]:
            if value in ("NAN", "NONE", ""):
                return ("", "")
            entry = self._country_lookup.get(value)
            if entry:
                return entry
            return (value, value)

        resolved = raw.apply(_lookup)
        df[iso3_col] = resolved.apply(lambda x: x[0])
        df[name_col] = resolved.apply(lambda x: x[1])

        return df

    def __repr__(self) -> str:
        flags = []
        if self.enrich_hs_codes:
            flags.append("hs_codes")
        if self.normalize_countries:
            flags.append("countries")
        return f"<EnrichmentTransformer enrichments={flags}>"
