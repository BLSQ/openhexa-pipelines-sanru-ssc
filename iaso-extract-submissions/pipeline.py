"""Pipeline for extracting and processing form submissions from IASO."""

from __future__ import annotations

import hashlib
import re
import unicodedata
from datetime import datetime
from io import StringIO
from pathlib import Path
import json
from typing import Any

import polars as pl
from openhexa.sdk import (
    DHIS2Connection,
    IASOConnection,
    current_run,
    parameter,
    pipeline,
    workspace,
)
from openhexa.sdk.datasets.dataset import Dataset, DatasetVersion
from openhexa.sdk.pipelines.parameter import IASOWidget
from openhexa.toolbox.iaso import IASO, dataframe
from openhexa.toolbox.dhis2 import DHIS2
from d2d_library.dhis2_pusher import DHIS2Pusher

# Precompile regex pattern for string cleaning
CLEAN_PATTERN = re.compile(r"[^\w\s-]")


@pipeline("iaso_extract_submissions")
@parameter(
    "iaso_connection", name="IASO connection", type=IASOConnection, required=True
)
@parameter(
    "form_id",
    name="Form ID",
    type=int,
    widget=IASOWidget.IASO_FORMS,
    connection="iaso_connection",
    required=True,
)
@parameter(
    "last_updated",
    name="Last Updated Date",
    type=str,
    required=False,
    help="ISO formatted date (YYYY-MM-DD) for incremental data extraction",
)
@parameter(
    "choices_to_labels",
    name="Convert Choices to Labels",
    type=bool,
    default=True,
    required=False,
    help="Replace choice codes with labels",
)
@parameter(
    code="output_file_name",
    type=str,
    name="Path and base name of the output file (without extension)",
    help=(
        "Path and base name of the output file (without extension) in the workspace files directory"
        "(default if ou_id is defined: "
        "`iaso-pipelines/extract-submissions/<form_name>.<output_format>`"
    ),
    required=False,
)
@parameter(
    code="output_format",
    type=str,
    name="File format to use for exporting the data",
    required=False,
    default=".parquet",
    choices=[
        ".csv",
        ".parquet",
        ".xlsx",
    ],
)
@parameter(
    "first_dataset_id",
    type=str,
    name="First Dataset ID",
    required=True,
)
@parameter(
    "second_dataset_id",
    type=str,
    name="Second Dataset ID",
    required=True,
)
@parameter(
    "third_dataset_id",
    type=str,
    name="Third Dataset ID",
    required=True,
)
@parameter(
    "dhis2_connection",
    type=DHIS2Connection,
    name="Third DHIS2 Connection",
    required=True,
)
@parameter(
    "dry_run",
    name="Output Dataset",
    type=bool,
    required=False,
    default=False,
    help="Target OpenHEXA dataset for storing submissions file",
)
def iaso_extract_submissions(
    iaso_connection: IASOConnection,
    form_id: int,
    last_updated: str | None,
    choices_to_labels: bool | None,
    output_file_name: str | None,
    output_format: str | None,
    first_dataset_id: str,
    second_dataset_id: str,
    third_dataset_id: str,
    dhis2_connection: DHIS2Connection,
    dry_run: bool,
):
    """Pipeline orchestration function for extracting and processing form submissions."""
    current_run.log_info("Starting form submissions extraction pipeline")

    # initialize_target dhis2 instance
    dhis2 = validate_connections(dhis2_connection)
    pusher = DHIS2Pusher(
        dhis2_client=dhis2,
        import_strategy="CREATE_AND_UPDATE",
        dry_run=dry_run,
        max_post=1000,
    )
    
    form_name = get_form_name(iaso_connection, form_id)
    output_file_path = extract_and_load(
        form_id,
        last_updated,
        choices_to_labels,
        output_file_name,
        output_format,
        form_name,
        iaso_connection,
    )
    place_holder = process_and_transform(
        form_name, output_format, output_file_name, output_file_path
    )

    post_to_first_dataset(pusher, dry_run, output_format, place_holder)
    # post_to_second_dataset(pusher, dry_run, output_format, place_holder)
    # post_to_third_dataset(pusher, dry_run, output_format, place_holder)


@iaso_extract_submissions.task
def extract_and_load(  # noqa: ANN201
    form_id: str,
    last_updated: str,
    choices_to_labels: str,
    output_file_name: str,
    output_format: str,
    form_name: str,
    iaso_connection: IASO,
):
    """Task to extract and save data to the workspace.

    Returns:
    ________
        output_file_path str: Path to where data is saved
    """
    try:

        cutoff_date = parse_cutoff_date(last_updated)
        iaso = authenticate_iaso(iaso_connection)
        submissions = fetch_submissions(iaso, form_id, cutoff_date)
        submissions = process_choices(submissions, choices_to_labels, iaso, form_id)
        submissions = deduplicate_columns(submissions)
        output_file_path = export_to_file(
            submissions, form_name, output_file_name, output_format
        )
        current_run.log_info(f"Data exported to file: `{output_file_path}`")

        current_run.log_info("Data extraction successful ✅")

        return output_file_path

    except Exception as exc:
        current_run.log_error(f"Pipeline failed: {exc}")
        raise


@iaso_extract_submissions.task
def process_and_transform(
    form_name: str, output_format: str, output_file_name: str, output_file_path: str
):
    """Task to process the data saved in the workspace."""
    try:
        # process the extracted data
        transform_data(output_format, form_name, output_file_name)
        current_run.log_info(f"Data exported to file: `{output_file_path}`")
    except Exception as exc:
        current_run.log_error(f"Pipeline failed: {exc}")
        raise


@iaso_extract_submissions.task
def post_to_first_dataset(
    pusher: DHIS2Pusher, dry_run: bool, output_format: str, place_holder: int
):
    """Task to post data to dhis2."""
    _post_handler(pusher, dry_run, output_format)


@iaso_extract_submissions.task
def post_to_second_dataset(
    pusher: DHIS2Pusher, dry_run: bool, output_format: str, place_holder: int
):
    """Task to post data to dhis2."""
    _post_handler(pusher, dry_run, output_format)


@iaso_extract_submissions.task
def post_to_third_dataset(
    pusher: DHIS2Pusher, dry_run: bool, output_format: str, place_holder: int
):
    """Task to post data to dhis2."""
    _post_handler(pusher, dry_run, output_format)


def _post_handler(pusher: DHIS2Pusher, dry_run: bool, output_format: str):
    folder_path = Path(workspace.files_path, "transformed")
    for transformed_file in list(folder_path.glob(f"*{output_format}")):
        if output_format == ".csv":
            transformed_data = pl.read_csv(
                transformed_file, infer_schema_length=1000, ignore_errors=True
            )
        elif output_format == ".parquet":
            transformed_data = pl.read_parquet(transformed_file)

        post_to_dhis2(pusher, transformed_data, dry_run)


def get_dhis2_client(connection: DHIS2Connection) -> DHIS2:
    """Initialize DHIS2 client with caching.

    Returns:
        DHIS2: Configured DHIS2 client instance.
    """
    cache_dir = Path(workspace.files_path) / ".cache"
    return DHIS2(connection=connection, cache_dir=cache_dir)


def validate_connections(
    dhis2_connection: DHIS2Connection,
) -> tuple[DHIS2, DHIS2]:
    """Validate source and target DHIS2 connections.

    Returns:
        tuple[DHIS2, DHIS2]: Source and target DHIS2 client instances.
    """
    current_run.log_info("Validating DHIS2 connections...")

    # Initialize clients
    dhis2 = get_dhis2_client(dhis2_connection)

    # Test source connection
    try:
        dhis2.ping()
        current_run.log_info(f"✓ Source DHIS2 connection successful: {dhis2.api.url}")
    except Exception as e:
        current_run.log_error(f"✗ Source DHIS2 connection failed: {e!s}")
        raise

    return dhis2


def yes_no_to_int(colname: str, alias: str) -> pl.Expr:
    """Convert yes/no or 1/0 values to an integer indicator column.

    Args:
        colname (str): Name of the source column to convert. The column
            will be cast to string before evaluation.
        alias (str): Name of the output column.

    Returns:
        pl.Expr: A Polars expression producing an integer column where
            "yes" or "1" maps to 1, "no" or "0" maps to 0, and all other
            values (including nulls) result in null.

    Raises:
        ValueError: If the column does not exist in the DataFrame
            (raised by Polars during query execution).
    """
    col = pl.col(colname).cast(pl.Utf8)

    return (
        pl.when(col.is_in(["1", "yes"]))
        .then(1)
        .when(col.is_in(["0", "no"]))
        .then(0)
        .otherwise(None)
        .alias(alias)
    )


def transform_data(  # noqa: D103
    output_format: str, form_name: str, output_file_name: str, process_all: bool = True
) -> None:
    # read data from raw

    with Path(workspace.files_path, "configurations/mappings.json").open(
        encoding="utf-8"
    ) as file:
        mapping_df = pl.DataFrame(json.loads(file.read()))

    folder_path = Path(workspace.files_path, "raw")

    for raw_file in list(folder_path.glob(f"*{output_format}")):
        if output_format == ".csv":
            supervision = pl.read_csv(
                raw_file, infer_schema_length=1000, ignore_errors=True
            )
        elif output_format == ".parquet":
            supervision = pl.read_parquet(raw_file)

        supervision = _process_columns(supervision)

        # TBD: only process the updated files. if process_all is True
        # TBD: process only files from a certain region/ province/ ou

        supervision = supervision.unpivot(
            index=["export_id", "periode"],
            variable_name="data_element",
            value_name="value",
        )
        supervision = supervision.rename(
            {"export_id": "organisation_unit_id", "periode": "period"}
        )
        supervision = supervision.with_columns(
            pl.col("data_element").str.to_lowercase()
        )
        mapping_df = mapping_df.with_columns(pl.col("name").str.to_lowercase())

        transformed_data = supervision.join(
            mapping_df, left_on="data_element", right_on="name", how="left"
        )

        transformed_data = transformed_data.drop(["Calcul", "data_element", "CC"])

        transformed_data = transformed_data.rename({"COC": "category_option_combo_id"})

        export_to_file(
            transformed_data.drop_nulls(),
            form_name,
            output_file_name,
            output_format,
            raw=False,
        )


def _process_columns(data: pl.DataFrame) -> pl.DataFrame:
    num = pl.Float64
    data = data.with_columns(
        [
            (
                pl.col("amoxy_qc_moins_de_1an").cast(num)
                + pl.col("amoxy_qc_1an_a_5ans").cast(num)
            ).alias("amoxy_qc_moins_de_1an_plus_amoxy_qc_1an_a_5ans"),
            (pl.col("sro_qc").cast(num) / 2).alias("sro_qc_divide_2"),
            (pl.col("sro_si").cast(num) / 2).alias("sro_si_divide_2"),
            (pl.col("sro_sd").cast(num) / 2).alias("sro_sd_divide_2"),
            (pl.col("sro_in").cast(num) / 2).alias("sro_in_divide_2"),
            (pl.col("sro_jrs").cast(num) / 2).alias("sro_jrs_divide_2"),
            (
                pl.col("paracetamol_500mg_moins1_qc").cast(num)
                + pl.col("paracetamol_500mg_1a3ans_qc").cast(num)
                + pl.col("paracetamol_500mg_3ansplus_qc").cast(num)
            ).alias(
                "paracetamol_500mg_moins1_qc_plus_paracetamol_500mg_1a3ans_qc_plus_paracetamol_500mg_3ansplus_qc"
            ),
            (
                pl.col("casorientcs-5_f").cast(num)
                + pl.col("casorientcs-5_g").cast(num)
            ).alias("casorientcs-5_f_plus_casorientcs-5_g"),
            (
                pl.col("casorientcs5plus_f").cast(num)
                + pl.col("casorientcs5plus_g").cast(num)
            ).alias("casorientcs5plus_f_plus_casorientcs5plus_g"),
            (
                pl.col("cascontreorient-5_f").cast(num)
                + pl.col("cascontreorient-5_g").cast(num)
            ).alias("cascontreorient-5_f_plus_cascontreorient-5_g"),
            (
                pl.col("cascontreorient5plus_f").cast(num)
                + pl.col("cascontreorient5plus_g").cast(num)
            ).alias("cascontreorient5plus_f_plus_cascontreorient5plus_g"),
            (
                pl.col("casprestb-5_f").cast(num) + pl.col("casprestb-5_g").cast(num)
            ).alias("casprestb-5_f_plus_casprestb-5_g"),
            (
                pl.col("cassusppalugrav-5_f").cast(num)
                + pl.col("cassusppalugrav-5_g").cast(num)
            ).alias("cassusppalugrav-5_f_plus_cassusppalugrav-5_g"),
            (
                pl.col("cassusppalugrav5plus_f").cast(num)
                + pl.col("cassusppalugrav5plus_g").cast(num)
            ).alias("cassusppalugrav5plus_f_plus_cassusppalugrav5plus_g"),
            (
                pl.col("castbconfirm-5_f").cast(num)
                + pl.col("castbconfirm-5_g").cast(num)
            ).alias("castbconfirm-5_f_plus_castbconfirm-5_g"),
            (
                pl.col("caseffetindes-5_f").cast(num)
                + pl.col("caseffetindes-5_g").cast(num)
                + pl.col("caseffetindes5plus_f").cast(num)
                + pl.col("caseffetindes5plus_g").cast(num)
            ).alias(
                "caseffetindes-5_f_plus_caseffetindes-5_g_plus_caseffetindes5plus_f_plus_caseffetindes5plus_g"
            ),
            (
                pl.col("pbrouge6-59_f").cast(num)
                + pl.col("pbrouge6-59_g").cast(num)
                + pl.col("pbjaune6-59_f").cast(num)
                + pl.col("pbjaune6-59_g").cast(num)
            ).alias(
                "pbrouge6-59_f_plus_pbrouge6-59_g_plus_pbjaune6-59_f_plus_pbjaune6-59_g"
            ),
        ]
    )

    return data.with_columns(
        [
            yes_no_to_int("minuteur_ari", "count yes minuteur_ari"),
            yes_no_to_int("minuteur_ari", "count yes minuteur_ari 2"),
            yes_no_to_int("muac", "count yes muac"),
            yes_no_to_int("muac", "count yes muac 2"),
            yes_no_to_int("poubelle_recep", "count yes poubelle_recep"),
            yes_no_to_int("caissemedexiste", "count yes caissemedexiste"),
            yes_no_to_int("caissesecure", "count yes caissesecure"),
        ]
    )


def authenticate_iaso(conn: IASOConnection) -> IASO:
    """Authenticates and returns an IASO object.

    Args:
        conn (IASOConnection): IASO connection details.

    Returns:
        IASO: An authenticated IASO object.
    """
    try:
        iaso = IASO(conn.url, conn.username, conn.password)
        current_run.log_info("IASO authentication successful")
        return iaso
    except Exception as exc:
        error_msg = f"IASO authentication failed: {exc}"
        current_run.log_error(error_msg)
        raise RuntimeError(error_msg) from exc


def get_form_name(iaso_connection: IASO, form_id: int) -> str:
    """Retrieve and sanitize form name.

    Args:
        iaso_connection (IASO): An authenticated IASO object.
        form_id (int): The ID of the form to check.

    Returns:
        str: Form name.

    Raises:
        ValueError: If the form does not exist.
    """
    iaso = authenticate_iaso(iaso_connection)
    try:
        response = iaso.api_client.get(
            f"/api/forms/{form_id}", params={"fields": {"name"}}
        )
        return clean_string(response.json().get("name"))
    except Exception as e:
        current_run.log_error(f"Form fetch failed: {e}")
        raise ValueError("Invalid form ID") from e


def parse_cutoff_date(date_str: str | None) -> str | None:
    """Validate and parse ISO date string.

    Args:
        date_str: Input date string in YYYY-MM-DD format

    Returns:
        Validated date string or None

    Raises:
        ValueError: For invalid date formats
    """
    if not date_str:
        return None

    try:
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError as exc:
        current_run.log_error("Invalid date format - must be YYYY-MM-DD")
        raise ValueError("Invalid date format") from exc


def fetch_submissions(
    iaso: IASO,
    form_id: int,
    cutoff_date: str | None,
) -> pl.DataFrame:
    """Retrieve form submissions from IASO API.

    Args:
        iaso: Authenticated IASO client
        form_id: Target form identifier
        cutoff_date: Optional date filter

    Returns:
        DataFrame containing form submissions
    """
    try:
        current_run.log_info(f"Fetching submissions for form ID {form_id}")
        csv = dataframe._get_instances_csv(iaso, form_id, last_updated=cutoff_date)
        return pl.read_csv(
            StringIO(csv),
            ignore_errors=True,
            infer_schema_length=10000,
        )
    except Exception as exc:
        current_run.log_error(f"Submission retrieval failed: {exc}")
        raise


def process_choices(
    submissions: pl.DataFrame, convert: bool, iaso_client: IASO, form_id: int
) -> pl.DataFrame:
    """Convert choice codes to human-readable labels if requested.

    Args:
        submissions: Raw submissions DataFrame
        convert: Conversion flag
        iaso_client: Authenticated IASO client
        form_id: Target form identifier

    Returns:
        Processed DataFrame with labels if requested
    """
    if not convert:
        return submissions

    try:
        form_metadata = dataframe.get_form_metadata(iaso_client, form_id)
        return dataframe.replace_labels(
            submissions=submissions, form_metadata=form_metadata, language="French"
        )
    except Exception as exc:
        current_run.log_error(f"Choice conversion failed: {exc}")
        raise


def deduplicate_columns(submissions: pl.DataFrame) -> pl.DataFrame:
    """Renames duplicate columns in the DataFrame by appending a unique suffix.

    Args:
        submissions: DataFrame with potential duplicate columns.

    Returns:
        pl.DataFrame: DataFrame with unique column names.
    """
    cleaned_columns = [clean_string(col) for col in submissions.columns]
    duplicates_columns = {
        k: list(range(1, cleaned_columns.count(k) + 1))
        for k in cleaned_columns
        if cleaned_columns.count(k) != 1
    }
    for col in cleaned_columns[:]:  # Iterate over a copy of the list
        if col in duplicates_columns:
            index = cleaned_columns.index(col)
            cleaned_columns[index] = f"{col}_{duplicates_columns[col][0]}"
            duplicates_columns[col].remove(duplicates_columns[col][0])

    submissions.columns = cleaned_columns
    return _process_submissions(submissions)


def export_to_file(
    submissions: pl.DataFrame,
    form_name: str,
    output_file_name: str,
    output_format: str,
    raw: bool = True,
) -> Path:
    """Export submissions data to specified file format.

    Args:
        submissions: DataFrame containing the form submissions.
        form_name: Name of the form to use in the output file name.
        output_file_name: Optional custom output file name. If not provided, defaults to
            `iaso-pipelines/extract-submissions/form_<form_name>.<output_format>`.
        output_format: File format extension for the output file.
        raw: Bolean to determine whether to write files in raw or transformed directories

    Returns:
        Path: The path to the exported file.
    """
    periods = submissions["periode" if raw else "period"].unique()
    for period in periods:
        output_file_path = _generate_output_file_path(
            form_name=form_name,
            output_file_name=(
                f"pipelines/iaso-extract-submissions/raw/{output_file_name}_{period}"
                if raw
                else f"pipelines/iaso-extract-submissions/transformed/{output_file_name}_{period}"
            ),
            output_format=output_format,
        )
        current_run.log_info(f"Exporting submissions fiile to: `{output_file_path}`")
        period_submission = submissions.filter(
            pl.col("periode" if raw else "period") == period
        )
        if output_format == ".csv":
            period_submission.write_csv(output_file_path)
        elif output_format == ".parquet":
            period_submission.write_parquet(output_file_path)
        else:
            period_submission.to_pandas().to_excel(output_file_path, index=False)

        # fpath = output_file_path.as_posix()
        # current_run.add_file_output(fpath)
    return output_file_path


def post_to_dhis2(
    pusher: DHIS2Pusher,
    transformed_data: pl.DataFrame,
    dry_run: bool,
) -> dict[str, Any]:
    """Post transformed data to target DHIS2 instance."""
    current_run.log_info(f"Posting data to target DHIS2 (dry_run={dry_run})...")

    if len(transformed_data) == 0:
        current_run.log_warning("No data to post after transformation")

    try:
        # Prepare payload
        payload = prepare_data_value_payload(transformed_data)
        current_run.log_info(f"Prepared {len(payload)} data values for posting")
        # Post data
        pusher.push_data(df_data=transformed_data.to_pandas())
        # Log results
        current_run.log_info("✓ Data posting completed")

    except Exception as e:

        current_run.log_error(f"Error posting data: {e}")
        raise


def prepare_data_value_payload(data_values: pl.DataFrame) -> list[dict[str, Any]]:
    """Prepare data values for DHIS2 API payload.

    Returns:
        list[dict[str, Any]]: List of data value dictionaries for API.
    """
    mapping_toolbox_dhis2_name = {
        "data_element_id": "dataElement",
        "organisation_unit_id": "orgUnit",
        "period": "period",
        "category_option_combo_id": "categoryOptionCombo",
        "value": "value",
        # "attribute_option_combo_id": "attributeOptionCombo",
    }

    # Check for required columns
    missing_columns = [
        col for col in mapping_toolbox_dhis2_name if col not in data_values.columns
    ]
    if missing_columns:
        current_run.log_error(f"Missing required columns: {missing_columns}")
        raise ValueError(f"Missing required columns: {missing_columns}")
    data_values = data_values.rename(mapping_toolbox_dhis2_name)
    # Convert to dictionaries
    payload = data_values.select(list(mapping_toolbox_dhis2_name.values())).to_dicts()

    # Convert values to strings as required by DHIS2
    valid_payload = []
    for item in payload:
        if any([value is None for value in item.values()]):
            print(f"Skipping item with None values: {item}")
            continue  # Skip items with None values
        if "value" in item and item["value"] is not None:
            item["value"] = str(item["value"])
            valid_payload.append(item)
    return valid_payload


def export_to_database(submissions: pl.DataFrame, table_name: str, mode: str) -> None:
    """Saves form submissions to a database.

    Args:
        submissions: DataFrame containing the form submissions.
        table_name: Name of the database table where submissions will be saved.
        mode: Mode to use when saving the table (replace or append).
    """
    if _validate_schema(submissions, table_name):
        mode = mode or "replace"
        submissions.write_database(
            table_name=table_name,
            connection=workspace.database_url,
            if_table_exists=mode,
        )
        current_run.add_database_output(table_name)
        current_run.log_info(
            f"Form submissions saved to database {len(submissions)} rows into `{table_name}`"
        )


def export_to_dataset(file_path: Path, dataset: Dataset | None) -> None:
    """Saves form submissions to the specified dataset.

    Args:
        file_path (Path): The path to the file containing the submissions data.
        dataset (Dataset): The dataset where the submissions will be stored.
    """
    latest_version = dataset.latest_version
    if bool(latest_version) and in_dataset_version(file_path, latest_version):
        current_run.log_info(
            f"Form submissions file `{file_path.name}` already exists in dataset version "
            f"`{latest_version.name}` and no changes have been detected"
        )
        return

    version_number = int(latest_version.name.lstrip("v")) + 1 if latest_version else 1
    version = dataset.create_version(f"v{version_number}")
    version.add_file(file_path, file_path.name)
    current_run.log_info(
        f"Form submissions file `{file_path.name}` successfully added to {dataset.name} "
        f"dataset in version `{version.name}`"
    )


def _process_submissions(submissions: pl.DataFrame) -> pl.DataFrame:
    """Process and clean the submissions DataFrame.

    Returns:
        pl.DataFrame: The cleaned and processed submissions DataFrame.
    """
    list_cols = submissions.select(pl.col(pl.List(pl.Utf8))).columns

    binary_exprs = []
    for col in list_cols:
        unique_cats = (
            submissions[col].drop_nulls().explode().drop_nulls().unique().to_list()
        )

        for cat in unique_cats:
            expr = (
                pl.col(col)
                .list.contains(cat)
                .fill_null(False)
                .cast(pl.Int8)
                .alias(f"{col}_{clean_string(cat)}")
            )
            binary_exprs.append(expr)

    if binary_exprs:
        submissions = submissions.with_columns(binary_exprs)

    submissions = submissions.drop(list_cols).select(
        pl.exclude("instanceid"), pl.col("instanceid")
    )

    return submissions.select(sorted(submissions.columns)).sort(submissions.columns)


def _validate_schema(submissions: pl.DataFrame, table_name: str) -> bool:
    """Validate the schema of the submissions DataFrame against the database table.

    Args:
        submissions: The submissions DataFrame to validate.
        table_name: Name of the database table to validate against.

    Returns:
        bool: True if schema is valid
    """
    db_table_columns = (
        pl.read_database_uri(
            query=(
                f"select column_name from information_schema.columns "
                f"where table_name='{table_name}'"
            ),
            uri=workspace.database_url,
        )
        .select("column_name")
        .to_series()
        .to_list()
    )
    if (
        db_table_columns
        and set(submissions.columns).issubset(set(db_table_columns))
        and len(submissions.columns) > len(db_table_columns)
    ):
        msg = (
            f"Schema mismatch: New columns {set(submissions.columns) - set(db_table_columns)} "
            "not present in existing table"
        )
        current_run.log_critical(msg)
        return False
    return True


def _generate_output_file_path(
    form_name: str, output_file_name: str, output_format: str
) -> Path:
    """Generate the output file path based on provided parameters.

    Args:
        form_name: Name of the form to include in the file name.
        output_file_name: Optional custom output file name.
        output_format: File format extension for the output file.

    Returns:
        Path to the output file.
    """
    if output_file_name:
        output_file_path = Path(output_file_name)

        if not output_file_path.suffix:
            output_file_path = output_file_path.with_suffix(output_format)

        if output_file_path.suffix not in [".csv", ".parquet", ".xlsx"]:
            current_run.log_error(
                f"Unsupported output format: {output_file_path.suffix}. "
                "Supported formats are: .csv, .parquet, .xlsx"
            )
            raise ValueError(f"Unsupported output format: {output_file_path.suffix}")

        if not output_file_path.is_absolute():
            output_file_path = Path(workspace.files_path, output_file_path)

        output_file_path.parent.mkdir(parents=True, exist_ok=True)
        return output_file_path

    output_dir = Path(workspace.files_path, "iaso-pipelines", "extract-submissions")
    output_dir.mkdir(exist_ok=True, parents=True)

    base_name = f"{clean_string(form_name)}"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H:%M")
    file_name = f"{base_name}_{timestamp}{output_format}"

    return output_dir / file_name


def clean_string(input_str: str) -> str:
    """Normalize and sanitize string for safe file/table names.

    Args:
        input_str: Original input string

    Returns:
        Normalized string with special characters removed
    """
    normalized = unicodedata.normalize("NFD", input_str)
    cleaned = "".join(c for c in normalized if not unicodedata.combining(c))
    sanitized = CLEAN_PATTERN.sub("", cleaned)
    return sanitized.strip().replace(" ", "_").lower()


def sha256_of_file(file_path: Path) -> str:
    """Calculate the SHA-256 hash of a file.

    Args:
        file_path (Path): Path to the file.

    Returns:
        str: SHA-256 hash of the file content.
    """
    hasher = hashlib.sha256()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def in_dataset_version(file_path: Path, dataset_version: DatasetVersion) -> bool:
    """Check if a file is in the specified dataset version.

    Args:
        file_path (Path): Path to the file.
        dataset_version (DatasetVersion): The dataset version to check against.

    Returns:
        bool: True if the file is in the dataset version, False otherwise.
    """
    file_hash = sha256_of_file(file_path)
    for file in dataset_version.files:
        remote_hash = hashlib.sha256()
        remote_hash.update(file.read())
        if file_hash == remote_hash.hexdigest():
            return True
    return False


if __name__ == "__main__":
    iaso_extract_submissions()
