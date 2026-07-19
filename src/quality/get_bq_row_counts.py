from google.cloud import bigquery

def get_bq_row_counts(client:bigquery.Client,table_ids:dict) -> dict:

    counts={}
    for label,table_id in table_ids.items():#loop over the labeled tables
        query=f"SELECT COUNT(*) as row_count FROM `{table_id}`" 
        result=client.query(query).result() # run the query
        row=next(iter(result)) # get the first row
        counts[f"{label}_row_count"]=row["row_count"] # add the row count to the counts dictionary

    return counts
