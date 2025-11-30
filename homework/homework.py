"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

from pathlib import Path
import pandas as pd

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    base_dir = Path(__file__).resolve().parents[1]
    input_dir = base_dir / "files" / "input"
    output_dir = base_dir / "files" / "output"
    output_dir.mkdir(parents=True, exist_ok=True)

    dfs = []
    for path in sorted(input_dir.glob("*.csv.zip")):
        df_tmp = pd.read_csv(path, compression="zip") 
        dfs.append(df_tmp)

    if not dfs:
        raise FileNotFoundError("No se encontraron archivos .csv.zip en files/input")

    df = pd.concat(dfs, ignore_index=True)
    df.columns = df.columns.str.strip()  

 
    client = df[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ].copy()

    client["job"] = (
        client["job"]
        .astype(str)
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )
    
    client["education"] = client["education"].astype(str).str.replace(
        ".", "_", regex=False
    )
    client["education"] = client["education"].replace("unknown", pd.NA)

    client["credit_default"] = (
        client["credit_default"].astype(str).str.lower().eq("yes")
    ).astype("int64")


    client["mortgage"] = (
        client["mortgage"].astype(str).str.lower().eq("yes")
    ).astype("int64")

    client = client[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ]

    client.to_csv(output_dir / "client.csv", index=False)

    # ------------------------------------------------------------------
    # 3. campaign.csv
    # ------------------------------------------------------------------
    campaign = df[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()


    campaign["previous_outcome"] = (
        campaign["previous_outcome"].astype(str).str.lower().eq("success")
    ).astype("int64")


    campaign["campaign_outcome"] = (
        campaign["campaign_outcome"].astype(str).str.lower().eq("yes")
    ).astype("int64")

    month_map = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    month_str = campaign["month"].astype(str).str.lower().map(month_map)
    day_str = campaign["day"].astype(int).astype(str).str.zfill(2)

    campaign["last_contact_date"] = "2022-" + month_str + "-" + day_str

    campaign = campaign.drop(columns=["day", "month"])


    campaign = campaign[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "last_contact_date",
        ]
    ]

    campaign.to_csv(output_dir / "campaign.csv", index=False)

    economics = df[["client_id", "cons_price_idx", "euribor_three_months"]].copy()

    economics = economics[["client_id", "cons_price_idx", "euribor_three_months"]]

    economics.to_csv(output_dir / "economics.csv", index=False)
    return


if __name__ == "__main__":
    clean_campaign_data()
