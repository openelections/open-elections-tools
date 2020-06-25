from open_elections.tools import StateDataFormat

national_precinct_dataformat = StateDataFormat(
    df_transformers=[lambda df: df.rename(columns={'election_district': 'precinct'})]
)
